var Subscription = require('./subscription');
var uuid = require('node-uuid');

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

	var subscription;
	var timer;

	function sendResponse(message) {
		if (subscription) {
			subscription.unsubscribe();
			subscription = undefined;
			if (message.err) {
				var newError = new Error();
				newError.name = message.err.name;
				newError.message = message.err.message;
				newError.stack = message.err.stack;
				message.err = newError;
			}
			callback(message.err, message.result);
		}
	}

	if (callback) {

		timer = !timeout ? undefined : setTimeout(function queueDidTimeout() {
			sendResponse({ err: new Error('timeout') });
		}, timeout);

		subscription = this.channel.subscribe(id, function handleMessage(message) {
			if (timer) {
				clearTimeout(timer);
				timer = undefined;
			}
			sendResponse(message);
		});
	}

	this.channel.publish(event, { id: id, data: data });
};

module.exports = exports = Queue;
