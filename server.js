var http = require('http'),
    qs = require('querystring'),
    url = require('url'),
    request = require('request'),
    config = require('./config.js');

var LONG_POOL_TIMEOUT = 25 * 1000; // 25 seconds
var EVENTS_EXPIRE_TIME = 60 * 1000; // 60 seconds

var events = [];
var pending = [];


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

function writeBadRequest(res) {
  res.writeHead(400);
  res.write('Bad Request');
  res.end();
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
        writeBadRequest(res);
      };

      try {
        var parsedData = JSON.parse(post.data);
      }
      catch (e) {
        writeBadRequest(res);
        return;
      }

      addEvent(post.event, parsedData);

      res.write('ok');
      res.end();
    });
  }
  else {
    writeBadRequest(res);
  }
}

function checkAccess(authToken, requestedEvents) {
  if (authToken.type == 'user') {
    // Этот тукен может подписаться на: message.APPID.USERID
    var allowedEvents = [
      'instantmessage.' + authToken.app + '.' + authToken.user,
    ];
  }


  // Проверим что все события, на которые подписывается - разрешены
  var isValid = true;
  var i = 0, j = 0;

  for (i = 0; i < requestedEvents.length; i++) {
    for (j = 0; j < allowedEvents.length; j++) {
      if (allowedEvents[j] != requestedEvents[i]) {
        isValid = false;
        break;
      }
    }
  }

  return isValid;
}

function listenerClient(req, res) {
  var u = url.parse(req.url, true);
  if (!u.query || !u.query.events || !u.query.auth_token) {
    writeBadRequest(res);
    return;
  }

  if (!u.query.timestamp)
    u.query.timestamp = currentTimestamp();

  request.get(config.API_ENDPOINT + '/auth/checktoken?auth_token=' + u.query.auth_token, function(error, response, body) {
    try {
      body = JSON.parse(body);
    }
    catch (e) {
      body = {meta: {status: 502}};
    }

    if (body.meta.status == 200 && body.data.active) {
      clearExpiredEvents();

      var requestedEvents = u.query.events.split(',');

      if (!checkAccess(body.data, requestedEvents)) {
        // Пытается подписаться на неразрешенное событие
        res.writeHead(403);
        res.write('Access Denied');
        res.end();
        return;
      }

      var immediateReturn = [];

      // Проверить, есть ли в базе нужные события, с таймстамп большим чем данный
      var i, j;
      for (i = 0; i < events.length; i++) {
        if (events[i].timestamp > u.query.timestamp) {
          for (j = 0; j < requestedEvents.length; j++) {
            if (events[i].event.startsWith(requestedEvents[j]))
              immediateReturn.push(events[i]);
          }
        }
      }

      if (immediateReturn.length > 0) {
        // Отправить сразу же
        sendEventsToClient(res, immediateReturn);
      }
      else {
        // Оставить как ждущего
        var timeout = setTimeout(function() {
          sendEventsToClient(res, []);

          for (var i = 0; i < pending.length; i++) {
            if (pending[i].res == res) {
              pending.splice(i, 1);
              break;
            }
          }
        }, LONG_POOL_TIMEOUT);

        pending.push({req: req, res: res, timeout: timeout, requestedEvents: requestedEvents});
      }
    }
    else if (body.meta.status == 403 || !body.data.active) {
      // Неверный или неактивный auth token
      res.writeHead(403);
      res.write('Access Denied');
      res.end();
    }
    else {
      // Something wrong
      res.writeHead(502);
      res.write('Something wrong');
      res.end();
    }
  });
}

function listenerStat(req, res) {
  res.write('Pending: ' + pending.length + ', Events: ' + events.length);
  res.end();
}

http.createServer(listenerClient).listen(config.PORT_USER);
http.createServer(listenerAPI).listen(config.PORT_API);
http.createServer(listenerStat).listen(config.PORT_STAT);
