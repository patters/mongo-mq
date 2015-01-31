var uuid = require('node-uuid');
var claims = require('./claims');

function Subscription(channel, event, oneway, callback) {
	if (!(this instanceof Subscription)) {
		return new Subscription(channel, event, oneway, callback);
	}

	var self = this;

	this.id = uuid.v4();
	this.channel = channel;
	this.event = event;
	this.oneway = oneway;
	this.callback = callback;

	this.subscription = channel.subscribe(event, function(message) {
		claims.claimMessage(channel, message.id, self.id, function(claimed) {
			self.onMessage(message.id, message.data);
		});
	});
}

Subscription.prototype.onMessage = function(id, data) {
	var channel = this.channel;
	if (!channel || this.callback) return;

	function respond(err, result) {
		if (!channel) return;
		channel.publish(id, { err: err, result: result });
	}

	if (this.oneway) {
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
