var uuid = require('node-uuid');

function Subscription(client, channel, event, oneway, callback) {
	if (!(this instanceof Subscription)) {
		return new Subscription(client, channel, event, oneway, callback);
	}

	var self = this;

	this.id = uuid.v4();
	this.client = client;
	this.channel = channel;
	this.event = event;
	this.oneway = oneway;
	this.callback = callback;

	this.subscription = channel.subscribe(event, function(message) {
		self.onMessage(message.id, message.data);
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
	delete this.client;
	delete this.channel;
	delete this.callback;
	delete this.subscription;
};