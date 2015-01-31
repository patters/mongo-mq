var chai = require('chai'),
	should = chai.should(),
	expect = chai.expect,
	assert = chai.assert,
	async = require('async'),
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

				var sub = queue.subscribe('event', function(message) {

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

				var sub = queue.subscribe('event', function(message) {
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

				var sub = queue.subscribe('event', function(message) {
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

	it('should be able to publish, receive, and respond to a message', function(done) {
		var connection = mq.createConnection(CONNECTION);

		connection.on('error', function(err) {
			should.not.exist(err);
			done();
		});

		connection.on('ready', function() {
			connection.queue('test', function(err, queue) {
				should.not.exist(err);
				should.exist(queue);

				var sub = queue.subscribe('event', function(message, callback) {
					expect(message).to.equal('hello');
					callback(null, message + ', world!');
				});

				should.exist(sub);

				queue.publish('event', 'hello', function(err, result) {
					should.not.exist(err);
					expect(result).to.equal('hello, world!');
					connection.close();
					done();
				});
			});
		});
	});

	it('should be able process messages once with multiple connections', function(done) {
		var pubConnection = mq.createConnection(CONNECTION);
		var subConnection1 = mq.createConnection(CONNECTION);
		var subConnection2 = mq.createConnection(CONNECTION);
		var conns = [pubConnection, subConnection1, subConnection2];

		conns.forEach(function(c) {
			c.on('error', function(err) {
				should.not.exist(err);
				done();
			});
		});

		async.map(conns, function(connection, callback) {
			connection.on('ready', function() {
				connection.queue('test', function(err, queue) {
					should.not.exist(err);
					should.exist(queue);
					callback(err, queue);
				});
			});

		}, function(err, queues) {
			should.not.exist(err);

			var got1, got2;

			var sub1 = queues[1].subscribe('event', function(message, callback) {
				expect(message).to.equal('hello');
				got1 = true;
				callback(null, message + ', 1!');
			});
			var sub2 = queues[2].subscribe('event', function(message, callback) {
				expect(message).to.equal('hello');
				got2 = true;
				callback(null, message + ', 2!');
			});

			var messages = 500;
			var received = 0;
			for (var sent = 0; sent < messages; sent++) {
				queues[0].publish('event', 'hello', function(err, result) {
					should.not.exist(err);
					expect(['hello, 1!', 'hello, 2!']).to.include(result);
					expect(received).to.be.below(messages);
					received++;
					if (received === messages) {
						expect(got1).to.be.true;
						expect(got2).to.be.true;
						// Wait a little more to ensure more messages don't come in
						setTimeout(function() {
							conns.forEach(function(c) { c.close(); });
							done();
						}, 500);
					}
				});
			}
		});
	});
});
