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
		});

		connection.on('ready', function() {
			connection.close();
			done();
		})
	});

	it('should be able to create a queue', function(done) {
		var connection = mq.createConnection(CONNECTION);

		connection.on('error', function(err) {
			should.not.exist(err);
		});

		connection.on('ready', function() {
			connection.queue('test', function(err, queue) {
				should.not.exist(err);
				should.exist(queue);
				connection.close();
				done();
			});
		});
	});

	it('should be able to create a subscription', function(done) {
		var connection = mq.createConnection(CONNECTION);

		connection.on('error', function(err) {
			should.not.exist(err);
		});

		connection.on('ready', function() {
			connection.queue('test', function(err, queue) {
				should.not.exist(err);
				should.exist(queue);

				var sub = queue.subscribe('event', true, function(message) {

				});

				should.exist(sub);

				connection.close();
				done();
			});
		});
	});

	it('should be able to publish and receive a message', function(done) {
		var connection = mq.createConnection(CONNECTION);

		connection.on('error', function(err) {
			should.not.exist(err);
			done();
		});

		connection.on('ready', function() {
			connection.queue('test', function(err, queue) {
				should.not.exist(err);
				should.exist(queue);

				var sub = queue.subscribe('event', true, function(message) {
					expect(message).to.equal('hello');
					connection.close();
					done();
				});

				should.exist(sub);

				queue.publish('event', 'hello');
			});
		});
	});

	it('should be able to publish and receive messages', function(done) {
		var connection = mq.createConnection(CONNECTION);

		connection.on('error', function(err) {
			should.not.exist(err);
			done();
		});

		connection.on('ready', function() {
			connection.queue('test', function(err, queue) {
				should.not.exist(err);
				should.exist(queue);

				var messages = 10;
				var received = 0;

				var sub = queue.subscribe('event', true, function(message) {
					expect(message).to.equal('hello');
					expect(received).to.be.below(messages);
					received++;
					if (received === messages) {
						connection.close();
						done();
					}
				});

				should.exist(sub);

				for (var sent = 0; sent < messages; sent++) {
					queue.publish('event', 'hello');
				}
			});
		});
	});
});
