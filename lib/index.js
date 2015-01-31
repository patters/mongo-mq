var connection = require('./connection');

module.exports = exports = {
	createConnection: function(options) {
		return connection(options);
	}
}
