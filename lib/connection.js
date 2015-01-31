var mubsub = require('mubsub');
var util = require('util');
var events = require('events');
var Queue = require('./queue');
var claims = require('./claims');

function Connection(options) {
	if (!(this instanceof Connection)) {
		return new Connection(options);
	}

	events.EventEmitter.call(this);

	var self = this;
	var client = options.client || mubsub(options);

	client.on('error', function(err) {
		self.emit('error', err);
	});

	client.on('connect', function(err) {
		self.emit('ready');
	});

	this.client = client;
}

Connection.prototype.queue = function(name, callback) {
	var channel = this.client.channel(name);

	// Fire the callback in response to a channel error
	// or after we're set up, but only once
	function _callback(err, result) {
		var cb = callback;
		callback = undefined;
		if (cb) cb(err, result);
	}
	channel.on('error', function(err) {
		self.emit('error', err);	
		_callback(err);
	});

	channel.on('ready', function() {
		claims.setupChannel(channel, function(err) {
			if (err) return _callback(err);
			_callback(null, new Queue(channel));
		});
	});
};

util.inherits(Connection, events.EventEmitter);

module.exports = exports = Connection;
