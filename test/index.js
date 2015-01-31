var chai = require('chai'),
	should = chai.should(),
	expect = chai.expect,
	assert = chai.assert,
	mq = require('../lib');

var CONNECTION = 'mongodb://localhost:27017/mubsub_example';

describe("# ", function() {

	it('should be able to create a connection', function(done) {
		var connection = mq.createConnection(CONNECTION);

		connection.on('error', function(err) {
			should.not.exist(err);
			done();
		});

		connection.on('ready', function() {
			done();
		})
	});
});
