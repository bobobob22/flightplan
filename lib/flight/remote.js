var Fiber = require('fibers')
  , Future = require('fibers/future')
  , extend = require('util-extend')
  , SSH = require('../transport/ssh')
  , format = require('util').format
  , logger = require('../logger')()
  , prettyTime = require('pretty-hrtime')
  , errors = require('../errors');

var _connections = [];

function connect(_context) {
  var future = new Future();

  new Fiber(function() {
    logger.info(format("Connecting to '%s' as '%s'",
      _context.remote.host, _context.remote.username));

    try {
      var connection = new SSH(_context);
      _connections.push(connection);
    } catch(e) {
      if(!_context.remote.failsafe) {
        throw new errors.ConnectionFailedError("Error connecting to '" +
                              _context.remote.host + "': " + e.message);
      }

      logger.warn("Safely failed connecting to '" +
                    _context.remote.host + "': " + e.message);
    }

    return future.return();
  }).run();

  return future;
}

function execute(transport, fn) {
  var future = new Future();

  new Fiber(function() {
    var t = process.hrtime();

    logger.info(format('Executing remote task on %s@%s',
      transport.runtime.username, transport.runtime.host));

    fn(transport);

    logger.info(format('Remote task on %s@%s finished after %s',
      transport.runtime.username, transport.runtime.host, prettyTime(process.hrtime(t))));

    return future.return();
  }).run();

  return future;
}

exports.execute = execute;

exports.run = function(fn, context) {
  if(_connections.length === 0) {
    Future.wait(context.hosts.map(function(host) {
      var _context = extend({}, context);
      _context.remote = host;

      return connect(_context);
    }));
  }

  Future.wait(_connections.map(function(connection) {
    return execute(connection, fn);
  }));
};

exports.disconnect = function() {
  _connections.forEach(function(connection) {
    connection.close();
  });

  _connections = [];
};
