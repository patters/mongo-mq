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

	it('should be able to publish, receive, and respond to a message (no timeout)', function(done) {
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

				queue.publish({ event: 'event', timeout: 0 }, 'hello', function(err, result) {
					should.not.exist(err);
					expect(result).to.equal('hello, world!');
					connection.close();
					done();
				});
			});
		});
	});

	it('should be able to publish, receive, and respond to a message (large timeout)', function(done) {
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

				queue.publish({ event: 'event', timeout: 1000 }, 'hello', function(err, result) {
					should.not.exist(err);
					expect(result).to.equal('hello, world!');
					connection.close();
					done();
				});
			});
		});
	});

	it('should be able process messages once with multiple connections', function(done) {
		this.timeout(15000);

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

			var messages = 1000;
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

	it('should be able to handle a timeout', function(done) {
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
					setTimeout(function() {
						callback(null, message + ', world!');
					}, 1000);
				});

				should.exist(sub);

				queue.publish({ event: 'event', timeout: 500 }, 'hello', function(err, result) {
					should.exist(err);
					expect(err).to.be.an.instanceof(Error);
					should.not.exist(result);
					// Wait a bit to ensure message doesn't get received later
					setTimeout(function() {
						connection.close();
						done();
					}, 1000);
				});
			});
		});
	});

	it('should be able to handle a stress test', function(done) {
		var connection = mq.createConnection(CONNECTION);

		connection.on('error', function(err) {
			should.not.exist(err);
			done();
		});

		connection.on('ready', function() {
			connection.queue('test', function(err, queue) {
				should.not.exist(err);
				should.exist(queue);

				queue.subscribe('main', function(data, callback) {
					//console.log('in catalog, data=' + data);
					var manifests = [1, 2, 3, 4, 5, 6];

					async.map(manifests, function(manifest, callback) {
						queue.publish('top', data + '.' + manifest, function(err, result) {
							//console.log('catalog got manifest err=' + err + ', result=' + result);
							callback(err, result);
						});
					}, function(err, results) {
						//console.log('catalog got all manifests, err=' + err + ', results=' + JSON.stringify(results));
						callback(err, JSON.stringify(results).replace(/"/g, ''));
					});
				});

				queue.subscribe('top', function(data, callback) {
					//console.log('in manifest, data=' + data);
					queue.publish('middle', data, function(err, result) {
						//console.log('manifest got issue err=' + err + ', result=' + result);
						callback(err, result + '.m');
					});
				});

				queue.subscribe('middle', function(data, callback) {
					//console.log('in issue, data=' + data);
					var pages = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
					async.map(pages, function(page, callback) {
						queue.publish('bottom', page, function(err, result) {
							//console.log('issue got page err=' + err + ', result=' + result);
							callback(err, result);
						});
					}, function(err, results) {
						results = results.join('');
						//console.log('issue got all pages, err=' + err + ', results=' + JSON.stringify(results));
						callback(err, JSON.stringify([data, results]).replace(/"/g, ''));
					});
				});

				queue.subscribe('bottom', function(data, callback) {
					callback(null, 'p' + data);
				});

				queue.publish({ event: 'main', timeout: 1000 }, 'test', function(err, result) {
					should.not.exist(err);
					//console.log(result);
					expect(result).to.equal('[[test.1,p1p2p3p4p5p6p7p8p9p10].m,[test.2,p1p2p3p4p5p6p7p8p9p10].m,[test.3,p1p2p3p4p5p6p7p8p9p10].m,[test.4,p1p2p3p4p5p6p7p8p9p10].m,[test.5,p1p2p3p4p5p6p7p8p9p10].m,[test.6,p1p2p3p4p5p6p7p8p9p10].m]');
					connection.close();
					done();
				});
			});
		});
	});
});
