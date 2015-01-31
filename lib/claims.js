
module.exports = exports = {

	collection: function(channel) {
		return channel.name + '_claims';
	},

	setupChannel: function(channel, callback) {
		var db = channel.connection.db;
		var collection = this.collection(channel);
		db.createCollection(collection, function(err, collection) {
			if (err) return callback(err);
			db.ensureIndex(collection, {
				message_id: 1
			}, {
				unique:true
			}, function(err, indexName) {
				if (err) return callback(err);
				db.ensureIndex(collection, {
					timestamp: 1
				}, {
					expireAfterSeconds: 60
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
			console.log('claimed message err=' + err + ', result=' + result);
			if (!err && result) {
				callback(true);
			} else {
				callback(false);
			}
		});
	}
}
