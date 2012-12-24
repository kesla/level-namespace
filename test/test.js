var test = require('tap').test;
var levelup = require('levelup');
var rimraf = require('rimraf');
var through = require('through');

var namespace = require('../namespace');
var dbPath = '/tmp/level-namespace-test';

function createArrayStream() {
    var array = [];
    return through(
        function write(data) {
            array.push(data);
        },
        function end() {
            this.emit('end', array);
        }
    );
}

var db;
test('level-namespace', function(t) {
    t.test('setup', function(t) {
        rimraf(dbPath, function() {
            levelup(dbPath, { createIfMissing: true }, function(err, _db) {
                t.equal(err, null);
                db = _db;
                namespace(db);
                t.end();
            });
        });
    });

    t.test('put', function(t) {
        t.plan(2);
        var space = db.namespace('put-test');
        space.put('foo', 'bar', function(err) {
            t.equal(err, null);
            db.get('put-test:foo', function(err, value) {
                t.equal(value, 'bar');
            })
        });
    });

    t.test('get', function(t) {
        t.plan(2);
        var space = db.namespace('get-test');
        db.put('get-test:foo', 'bar', function(err) {
            t.equal(err, null);
            space.get('foo', function(err, value) {
                t.equal(value, 'bar');
            })
        });
    });

    t.test('del', function(t) {
        t.plan(5);
        var space = db.namespace('del-test');
        db.put('del-test:foo', 'bar', function(err) {
            t.equal(err, null);
            space.del('foo', function(err) {
                t.equal(err, null);
                db.get('del-test:foo', function(err, value) {
                    t.equal(value, undefined);
                    t.type(err, Error);
                    t.equal(err.name, 'NotFoundError');
                });
            })
        });
    });

    t.test('batch', function(t) {
        var space = db.namespace('batch-test');
        var input = [
            { type: 'put', key: 'hello', value: 'world' },
            { type: 'put', key: 'foo', value: 'bar' }
        ];
        space.batch(input, function(err) {
            t.equal(err, null);
            t.equal(input[0].key, 'hello');
            t.equal(input[1].key, 'foo');
            db.get('batch-test:hello', function(err, value) {
                t.equal(err, null);
                t.equal(value, 'world');
                t.end();
            });
        });
    });

    t.test('readable streams', function(t) {
        t.test('setup', function(t) {
            db.batch([
                { type: 'put', key: 'foo', value: 'baz' },
                { type: 'put', key: 'read-stream-test:foo', value: 'bar' },
                { type: 'put', key: 'read-stream-test:hello', value: 'world' },
                { type: 'put', key: 'read-stream-test;hello', value: 'worldz' }
            ], function(err) {
                t.equal(err, null);
                t.end();
            });
        });

        var space = db.namespace('read-stream-test');

        t.test('keyStream', function(t) {
            space.keyStream().pipe(createArrayStream()).once('end', function(array) {
                t.deepEqual(array, [ 'foo', 'hello' ]);
                t.end();
            });
        });
        t.test('valueStream', function(t) {
            space.valueStream().pipe(createArrayStream()).once('end', function(array) {
                t.deepEqual(array, [ 'bar', 'world' ]);
                t.end();
            });
        });

        t.test('readStream', function(t) {
            space.readStream().pipe(createArrayStream()).once('end', function(array) {
                t.deepEqual(array, [
                    { key: 'foo', value: 'bar' },
                    { key: 'hello', value: 'world' }
                ]);
                t.end();
            });
        });
        t.end();
    });

    t.test('writable streams', function(t) {
        t.plan(2);
        var space = db.namespace('write-stream-test');
        var stream = space.writeStream();
        stream.once('close', function() {
            process.nextTick(function() {
                db.get('write-stream-test:hello', function(err, value) {
                    t.equal(err, null);
                    t.equal(value, 'world');
                });
            });
        });
        stream.write({ key: 'hello', value: 'world' })
        stream.write({ key: 'foo', value: 'bar'})
        stream.end()
    });
});