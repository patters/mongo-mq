var Subscription = require('./subscription');
var uuid = require('node-uuid');

var TIMEOUT_ERROR = new Error('timeout');

function Queue(channel) {
	if (!(this instanceof Queue)) {
		return new Queue(channel);
	}

	this.channel = channel;
}

Queue.prototype.subscribe = function(event, callback) {
	return new Subscription(this.channel, event, callback);
};

Queue.prototype.publish = function(eventOrOptions, data, callback) {

	var event = eventOrOptions.event || eventOrOptions;
	var timeout = eventOrOptions.timeout || 0;
	var id = uuid.v4();
	
	if (callback) {
		var subscription;
		var timer;

		function responded(message) {
			if (subscription) {
				subscription.unsubscribe();
				subscription = undefined;
				callback(message.err, message.result);
			}
		}

		timer = !timeout ? undefined : setTimeout(function() {
			responded({ err: TIMEOUT_ERROR });
		}, timeout);

		subscription = this.channel.subscribe(id, function(message) {
			if (timer) {
				clearTimeout(timer);
				timer = undefined;
			}
			responded(message);
		});
	}

	this.channel.publish(event, { id: id, data: data });
};

module.exports = exports = Queue;
