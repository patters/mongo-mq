
module.exports = exports = {

	collection: function(channel) {
		return channel.name + '_claims';
	},

	setupChannel: function(channel, callback) {
		var db = channel.connection.db;
		db.createCollection(this.collection(channel), function(err, collection) {
			if (err) return callback(err);
			collection.ensureIndex({
				message_id: 1
			}, {
				unique:true
			}, function(err, indexName) {
				if (err) return callback(err);
				collection.ensureIndex({
					timestamp: 1
				}, {
					expireAfterSeconds: 600
				}, function(err, indexName) {
					callback(err);
				});
			});
		});
	},

	claimMessage: function(channel, message, subscription, callback) {
		if (!channel || !message || !subscription) return callback(false);

		var collection = this.collection(channel);
		var db = channel.connection.db;
		
		if (!db) return callback(false);

		db.collection(collection).insert({
			message_id: message,
			subscription_id: subscription,
			timestamp: new Date()
		}, function(err, result) {
			if (!err && result) {
				callback(true);
			} else {
				callback(false);
			}
		});
	}
}
