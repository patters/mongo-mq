var uuid = require('node-uuid');
var claims = require('./claims');

function Subscription(channel, event, callback) {
	if (!(this instanceof Subscription)) {
		return new Subscription(channel, event, callback);
	}

	var self = this;

	this.id = uuid.v4();
	this.channel = channel;
	this.event = event;
	this.callback = callback;

	this.subscription = channel.subscribe(event, function(message) {
		claims.claimMessage(channel, message.id, self.id, function(claimed) {
			if (claimed) {
				self.onMessage(message.id, message.data);
			}
		});
	});
}

Subscription.prototype.onMessage = function(id, data) {
	var channel = this.channel;

	if (!channel || !this.callback) return;

	function respond(err, result) {
		if (!channel) return;
		if (err) {
			var newError = { name: err.name, message: err.message, stack: err.stack };
			err = newError;
		}
		channel.publish(id, { err: err, result: result });
	}

	if (this.callback.length === 1) {
		this.callback(data);
	} else {
		this.callback(data, respond);
	}
};

Subscription.prototype.unsubscribe = function() {
	this.subscription.unsubscribe();
	delete this.channel;
	delete this.callback;
	delete this.subscription;
};

module.exports = exports = Subscription;
