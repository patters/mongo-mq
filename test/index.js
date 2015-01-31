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
		this.timeout(15000);

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

				queue.publish({ event: 'main', timeout: 10000 }, 'test', function(err, result) {
					should.not.exist(err);
					//console.log(result);
					expect(result).to.equal('[[test.1,p1p2p3p4p5p6p7p8p9p10].m,[test.2,p1p2p3p4p5p6p7p8p9p10].m,[test.3,p1p2p3p4p5p6p7p8p9p10].m,[test.4,p1p2p3p4p5p6p7p8p9p10].m,[test.5,p1p2p3p4p5p6p7p8p9p10].m,[test.6,p1p2p3p4p5p6p7p8p9p10].m]');
					connection.close();
					done();
				});
			});
		});
	});


	it('should be able to handle a really stressful test', function(done) {
		var connection = mq.createConnection(CONNECTION);
		this.timeout(15000);

		connection.on('error', function(err) {
			should.not.exist(err);
			done();
		});

		connection.on('ready', function() {
			connection.queue('test', function(err, queue) {
				should.not.exist(err);
				should.exist(queue);

				function subscriberLevel(level) {
					return queue.subscribe('level' + level, function(data, callback) {
						async.map([2 * data, 2 * data + 1], function(data, callback) {
							queue.publish('level' + (level + 1), data, callback);
						}, function(err, results) {
							callback(err, results);
						});
					});
				}

				var level;
				for (level = 1; level < 10; level++) {
					subscriberLevel(level);
				}

				queue.subscribe('level' + level, function(data, callback) {
					callback(null, data);
				});

				queue.publish('level1', 1, function(err, result) {
					should.not.exist(err);
					expect(JSON.stringify(result)).to.equal('[[[[[[[[[512,513],[514,515]],[[516,517],[518,519]]],[[[520,521],[522,523]],[[524,525],[526,527]]]],[[[[528,529],[530,531]],[[532,533],[534,535]]],[[[536,537],[538,539]],[[540,541],[542,543]]]]],[[[[[544,545],[546,547]],[[548,549],[550,551]]],[[[552,553],[554,555]],[[556,557],[558,559]]]],[[[[560,561],[562,563]],[[564,565],[566,567]]],[[[568,569],[570,571]],[[572,573],[574,575]]]]]],[[[[[[576,577],[578,579]],[[580,581],[582,583]]],[[[584,585],[586,587]],[[588,589],[590,591]]]],[[[[592,593],[594,595]],[[596,597],[598,599]]],[[[600,601],[602,603]],[[604,605],[606,607]]]]],[[[[[608,609],[610,611]],[[612,613],[614,615]]],[[[616,617],[618,619]],[[620,621],[622,623]]]],[[[[624,625],[626,627]],[[628,629],[630,631]]],[[[632,633],[634,635]],[[636,637],[638,639]]]]]]],[[[[[[[640,641],[642,643]],[[644,645],[646,647]]],[[[648,649],[650,651]],[[652,653],[654,655]]]],[[[[656,657],[658,659]],[[660,661],[662,663]]],[[[664,665],[666,667]],[[668,669],[670,671]]]]],[[[[[672,673],[674,675]],[[676,677],[678,679]]],[[[680,681],[682,683]],[[684,685],[686,687]]]],[[[[688,689],[690,691]],[[692,693],[694,695]]],[[[696,697],[698,699]],[[700,701],[702,703]]]]]],[[[[[[704,705],[706,707]],[[708,709],[710,711]]],[[[712,713],[714,715]],[[716,717],[718,719]]]],[[[[720,721],[722,723]],[[724,725],[726,727]]],[[[728,729],[730,731]],[[732,733],[734,735]]]]],[[[[[736,737],[738,739]],[[740,741],[742,743]]],[[[744,745],[746,747]],[[748,749],[750,751]]]],[[[[752,753],[754,755]],[[756,757],[758,759]]],[[[760,761],[762,763]],[[764,765],[766,767]]]]]]]],[[[[[[[[768,769],[770,771]],[[772,773],[774,775]]],[[[776,777],[778,779]],[[780,781],[782,783]]]],[[[[784,785],[786,787]],[[788,789],[790,791]]],[[[792,793],[794,795]],[[796,797],[798,799]]]]],[[[[[800,801],[802,803]],[[804,805],[806,807]]],[[[808,809],[810,811]],[[812,813],[814,815]]]],[[[[816,817],[818,819]],[[820,821],[822,823]]],[[[824,825],[826,827]],[[828,829],[830,831]]]]]],[[[[[[832,833],[834,835]],[[836,837],[838,839]]],[[[840,841],[842,843]],[[844,845],[846,847]]]],[[[[848,849],[850,851]],[[852,853],[854,855]]],[[[856,857],[858,859]],[[860,861],[862,863]]]]],[[[[[864,865],[866,867]],[[868,869],[870,871]]],[[[872,873],[874,875]],[[876,877],[878,879]]]],[[[[880,881],[882,883]],[[884,885],[886,887]]],[[[888,889],[890,891]],[[892,893],[894,895]]]]]]],[[[[[[[896,897],[898,899]],[[900,901],[902,903]]],[[[904,905],[906,907]],[[908,909],[910,911]]]],[[[[912,913],[914,915]],[[916,917],[918,919]]],[[[920,921],[922,923]],[[924,925],[926,927]]]]],[[[[[928,929],[930,931]],[[932,933],[934,935]]],[[[936,937],[938,939]],[[940,941],[942,943]]]],[[[[944,945],[946,947]],[[948,949],[950,951]]],[[[952,953],[954,955]],[[956,957],[958,959]]]]]],[[[[[[960,961],[962,963]],[[964,965],[966,967]]],[[[968,969],[970,971]],[[972,973],[974,975]]]],[[[[976,977],[978,979]],[[980,981],[982,983]]],[[[984,985],[986,987]],[[988,989],[990,991]]]]],[[[[[992,993],[994,995]],[[996,997],[998,999]]],[[[1000,1001],[1002,1003]],[[1004,1005],[1006,1007]]]],[[[[1008,1009],[1010,1011]],[[1012,1013],[1014,1015]]],[[[1016,1017],[1018,1019]],[[1020,1021],[1022,1023]]]]]]]]]');
					connection.close();
					done();
				});
			});
		});
	});
});
