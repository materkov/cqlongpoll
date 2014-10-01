var http = require('http'),
    qs = require('querystring'),
    url = require('url'),
    request = require('request'),
    config = require('./config.js');

var LONG_POLL_TIMEOUT = 25 * 1000;  // 25 seconds
var EVENTS_EXPIRE_TIME = 10 * 60 * 1000;  // 10 minutes
var AUTH_TOKEN_CACHE_TIME = 30 * 60 * 1000;  // 30 minutes
var AUTH_TOKEN_OFFLINE_TIMEOUT = 10 * 1000;  // 10 seconds

var events = [];
var pending = [];
var tokenCache = {};
var tokenOfflineTimeouts = {};


function currentTimestamp() {
  return new Date().getTime();
}

// Helper function
if (typeof String.prototype.startsWith != 'function') {
  String.prototype.startsWith = function (str) {
    return this.slice(0, str.length) == str;
  };
}

function clearExpiredEvents() {
  var deleteTime = currentTimestamp() - EVENTS_EXPIRE_TIME;
  var deleteIndex = -1;

  for (var i = 0; i < events.length; i++) {
    if (events[i].timestamp < deleteTime)
      deleteIndex = i;
    else
      break;
  }

  if (deleteIndex != -1)
    events.splice(0, deleteIndex + 1);
}

function sendEventsToClient(res, events) {
  if (events.length > 0)
    var _timestamp = events[events.length-1].timestamp
  else
    var _timestamp = currentTimestamp();

  var response = {
    meta: {
      status: 200
    },
    data: {
      events: events,
      timestamp: _timestamp
    }
  };

  res.writeHead(200, {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
  });
  res.write(JSON.stringify(response, null, 2));
  res.end();
}

function checkPendingForEvent(event) {
  var i = 0, j = 0;

  for (i = 0; i < pending.length; i++) {
    for (j = 0; j < pending[i].requestedEvents.length; j++) {
      if (event.event.startsWith(pending[i].requestedEvents[j])) {
        sendEventsToClient(pending[i].res, [event]);
        setOffline(pending[i].authToken);
        
        // Удаляем клиента из ожидающих
        clearTimeout(pending[i].timeout);
        pending.splice(i, 1);
        i--;
        break;
      }
    }
  }
}

function addEvent(event, data) {
  // Добавить в очередь
  var _event = {
    event: event,
    timestamp: currentTimestamp(),
    data: data
  };
  events.push(_event);

  // Проверить, кто подписан на это событие, тут же отправить его
  clearExpiredEvents();
  checkPendingForEvent(_event);
}

function writeError(res, error) {
  res.writeHead(error, {
    "Access-Control-Allow-Origin": "*",
  });
  
  if (error == 400)
    res.write('Bad Request');
  else if (error == 403)
    res.write('Access denied');
  else if (error == 502)
    res.write('Something wrong');
  
  res.end();
}

function tokenOfflineTimeoutCancel(authToken) {
  if (tokenOfflineTimeouts.hasOwnProperty(authToken)) {
    // Если для этого тукена был поставлен таймер на оффлайн, то получается, что он заново поключился.
    // Нужно отменить таймер.
    clearTimeout(tokenOfflineTimeouts[authToken]);
    delete tokenOfflineTimeouts[authToken];

    return true;
  }

  return false;
}

// Вызывается когда новое соединение
function setOnline(authToken) {
  if (authToken.indexOf('user') != 0) return;  // Нужно это только для user тукенов

  var deleted = tokenOfflineTimeoutCancel(authToken);
  if (!deleted) {
    // Устанавливать онлайн нужно только в том случае, если НЕ был удален
    // Потому что, если был удален, значит, уже подключался раньше и онлайн 
    // уже был установлен для него, устанавливать еще раз не нужно
    var operations = [{op: 'update_or_create', key: '$online', value: true}];
    var params = {app: '$self_app', operations: JSON.stringify(operations)};
    request.post(config.API_ENDPOINT + '/users/$self_user/setproperties?auth_token=' + authToken, {form: params});
  }
}

// Вызывается когда закрывается соединение
function setOffline(authToken) {
  if (authToken.indexOf('user') != 0) return;  // Нужно это только для user тукенов

  tokenOfflineTimeoutCancel(authToken);

  tokenOfflineTimeouts[authToken] = setTimeout(function() {
    delete tokenOfflineTimeouts[authToken];

    // Если в pending еще есть юзер с таким id, то не надо ставить оффлайн
    var foundInPending = false;
    for (var i = 0; i < pending.length; i++) {
      if (pending[i].authToken == authToken) {
        foundInPending = true;
        break;
      }
    }
    
    if (foundInPending) return;

    var operations = [{op: 'update_or_create', key: '$online', value: false}];
    var params = {app: '$self_app', operations: JSON.stringify(operations)};
    request.post(config.API_ENDPOINT + '/users/$self_user/setproperties?auth_token=' + authToken, {form: params});
  }, AUTH_TOKEN_OFFLINE_TIMEOUT);
}

/*
 * Прослушивает порт добавления событий
 */
function listenerAPI(req, res) {
  if (req.method == 'POST') {
    var body = '';
    req.on('data', function (data) {
      body += data;

      // Too much POST data, kill the connection!
      if (body.length > 1e6)
        req.connection.destroy();
    });
    req.on('end', function () {
      var post = qs.parse(body);
      if (!post.event || !post.data) {
        writeError(res, 400);
      };

      try {
        var parsedData = JSON.parse(post.data);
      }
      catch (e) {
        writeError(res, 400);
        return;
      }

      addEvent(post.event, parsedData);

      res.write('ok');
      res.end();
    });
  }
  else {
    writeError(res, 4000);
  }
}

function getTokenInfo(authToken, callback) {
  var cached = tokenCache[authToken];

  // Check expired
  if (cached && cached._timestamp < (currentTimestamp() - AUTH_TOKEN_CACHE_TIME))
    cached = null;

  if (!cached) {
    request.get(config.API_ENDPOINT + '/auth/checktoken?auth_token=' + authToken, function(error, response, body) {
      try {
        if (!response || response.statusCode != 200) throw 'Error';

        body = JSON.parse(body);
        tokenCache[authToken] = body;
        tokenCache[authToken]._timestamp = currentTimestamp();
      }
      catch (e) {
        body = {meta: {status: 502}, data: {}};
      }

      callback(body);
    });
  }
  else {
    callback(cached);
  }
}

// Вернуть события, которые допустимы для данного тукена
function getAllowedEventsForToken(authToken) {
  var allowedEvents = [];

  if (authToken.type == 'user') {
    allowedEvents.push('instantmessage.' + authToken.app + '.' + authToken.user);
    allowedEvents.push('user_status_change.' + authToken.app + '.' + authToken.user);
    allowedEvents.push('instantmessage_read.' + authToken.app + '.' + authToken.user);
    allowedEvents.push('campaign_hit.' + authToken.app + '.' + authToken.user);

    allowedEvents.push('conversation.' + authToken.app + '.' + authToken.user);
    allowedEvents.push('conversation_reply.' + authToken.app + '.' + authToken.user);
    allowedEvents.push('conversation_read.' + authToken.app + '.' + authToken.user);
  }
  else if (authToken.type == 'panel') {
    for (var i = 0; i < authToken.apps.length; i++) {
      allowedEvents.push('instantmessage.' + authToken.apps[i] + '.');
      allowedEvents.push('user_status_change.' + authToken.apps[i] + '.');
      allowedEvents.push('instantmessage_read.' + authToken.apps[i] + '.');
      allowedEvents.push('campaign_hit.' + authToken.apps[i] + '.');

      allowedEvents.push('conversation.' + authToken.apps[i] + '.');
      allowedEvents.push('conversation_reply.' + authToken.apps[i] + '.');
      allowedEvents.push('conversation_read.' + authToken.apps[i] + '.');
    }
  };

  return allowedEvents;
}

function checkAccess(authToken, requestedEvents) {
  var allowedEvents = getAllowedEventsForToken(authToken);

  // Проверим что все события, на которые подписывается - разрешены
  var isValid = true, i = 0;

  for (i = 0; i < requestedEvents.length; i++) {
    if (allowedEvents.indexOf(requestedEvents[i]) == -1) {
      isValid = false;
      break;
    };
  }

  return isValid;
}

function getUIDFromToken(authToken) {
  var UID_REGEXP = /user\.(\d+)\./;
  var match = UID_REGEXP.exec(authToken);
  if (match)
    return parseInt(match[1]);
}

function listenerClient(req, res) {
  var u = url.parse(req.url, true);
  if (!u.query || !u.query.events || !u.query.auth_token) {
    writeError(res, 400);
    return;
  }

  u.query.timestamp = parseInt(u.query.timestamp);  // Force integer convert
  if (!u.query.timestamp)
    u.query.timestamp = currentTimestamp();

  getTokenInfo(u.query.auth_token, function(body) {
    if (body.meta.status == 200 && body.data.active) {
      clearExpiredEvents();

      var requestedEvents = u.query.events.split(',');

      if (!checkAccess(body.data, requestedEvents)) {
        // Пытается подписаться на неразрешенное событие
        writeError(res, 403);
        return;
      }

      var immediateReturn = [];

      // Проверить, есть ли в базе нужные события, с таймстамп большим чем данный
      var i, j;
      for (i = 0; i < events.length; i++) {
        if (events[i].timestamp > u.query.timestamp) {
          for (j = 0; j < requestedEvents.length; j++) {
            if (events[i].event.startsWith(requestedEvents[j])) {
              immediateReturn.push(events[i]);
            }
          }
        }
      }

      if (immediateReturn.length > 0) {
        // Отправить сразу же
        sendEventsToClient(res, immediateReturn);
      }
      else {
        // Выясним таймаут, если указано, что не ждать, то таймаут поставим ноль.
        var neededTimeout = LONG_POLL_TIMEOUT;
        if (u.query.no_wait)
          neededTimeout = 0;

        // Оставить как ждущего
        var timeout = setTimeout(function() {
          sendEventsToClient(res, []);
          setOffline(u.query.auth_token);

          for (var i = 0; i < pending.length; i++) {
            if (pending[i].res == res) {
              pending.splice(i, 1);
              break;
            }
          }
        }, neededTimeout);

        req.on('close', function() {
          // Клиент закрыл соединение
          setOffline(u.query.auth_token);

          for (var i = 0; i < pending.length; i++) {
            if (pending[i].res == res) {
              clearTimeout(pending[i].timeout);
              pending.splice(i, 1);
              break;
            }
          }
        });

        pending.push({
          req: req, 
          res: res, 
          timeout: timeout, 
          requestedEvents: requestedEvents, 
          authToken: u.query.auth_token,
          uid: getUIDFromToken(u.query.auth_token)});
        setOnline(u.query.auth_token);
      }
    }
    else if (body.meta.status == 400 || (body.meta.status == 200 && !body.data.active)) {
      // Неверный или неактивный auth token
      writeError(res, 403);
    }
    else {
      // Something wrong
      writeError(res, 502);
    }
  });
}

function listenerStat(req, res) {
  var u = url.parse(req.url, true);
  if (u.query && u.query.users) {
    var usersPending = [], usersOfflineTimeout = [];

    for (var i = 0; i < pending.length; i++) {
      var uid = getUIDFromToken(pending[i].authToken);
      if (uid)
        usersPending.push(uid);
    }

    for (var key in tokenOfflineTimeouts) {
      var uid = getUIDFromToken(key);
      if (uid)
        usersOfflineTimeout.push(uid);
    }

    res.write(JSON.stringify({'pending': usersPending, 'offlinetimeouts': usersOfflineTimeout}));
    res.end();
  }
  else {
    res.write('Pending: ' + pending.length + ', Events: ' + events.length);
    res.end();
  }
}

http.createServer(listenerClient).listen(config.PORT_USER);
http.createServer(listenerAPI).listen(config.PORT_API);
http.createServer(listenerStat).listen(config.PORT_STAT);
