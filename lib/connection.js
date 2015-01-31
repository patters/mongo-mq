var mubsub = require('mubsub');
var util = require('util');
var events = require('events');
var Queue = require('./queue');

function Connection(options) {
	if (!(this instanceof Connection)) {
		return new Connection(options);
	}

	events.EventEmitter.call(this);

	var client = options.client || mubsub(options);

	client.on('error', function(err) {
		self.emit('error', err);
	});

	client.on('connect', function(err) {
		// TODO: set up claims collection
		self.emit('ready');
	});

	this.client = client;
}

Connection.prototype.queue = function(name, callback) {
	var channel = this.client.channel(name);

	channel.on('error', function(err) {
		self.emit('error', err);		
	});

	channel.on('ready', function() {
		callback(new Queue(this.client, channel));
	});
};

util.inherits(Connection, events.EventEmitter);

module.exports = exports = Connection;
