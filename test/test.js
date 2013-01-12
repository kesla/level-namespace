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
        var space = db.namespace('put:test');
        space.put('foo', 'bar', function(err) {
            t.equal(err, null);
            db.get('put%3Atest:foo', function(err, value) {
                t.equal(err, null);
                t.equal(value, 'bar');
                t.end();
            })
        });
    });

    t.test('put with opts', function(t) {
        var space = db.namespace('put:opts:test');
        var opts = { valueEncoding: 'json' };
        var json = { hello: 'world' };
        space.put('foo2', json, opts, function(err) {
            t.equal(err, null);
            db.get('put%3Aopts%3Atest:foo2', opts, function(err, value) {
                t.equal(err, null);
                t.deepEqual(value, json);
                t.end();
            });
        });
    });

    t.test('get', function(t) {
        var space = db.namespace('get:test');
        db.put('get%3Atest:foo', 'bar', function(err) {
            t.equal(err, null);
            space.get('foo', function(err, value) {
                t.equal(err, null);
                t.equal(value, 'bar');
                t.end();
            })
        });
    });

    t.test('get with opts', function(t) {
        var space = db.namespace('get:opts:test');
        var opts = { valueEncoding: 'json' };
        var json = { hello: 'world' };
        db.put('get%3Aopts%3Atest:foo', json, opts, function(err) {
            t.equal(err, null);
            space.get('foo', opts, function(err, value) {
                t.equal(err, null);
                t.deepEqual(value, json);
                t.end();
            });
        });
    });

    t.test('del', function(t) {
        var space = db.namespace('del:test');
        db.put('del%3Atest:foo', 'bar', function(err) {
            t.equal(err, null);
            space.del('foo', function(err) {
                t.equal(err, null);
                db.get('del%3Atest:foo', function(err, value) {
                    t.equal(value, undefined);
                    t.type(err, Error);
                    if (err) t.equal(err.name, 'NotFoundError');
                    t.end();
                });
            })
        });
    });

    t.test('del with opts', function(t) {
        var space = db.namespace('del:opts:test');
        var opts = { sync: true };
        db.put('del%3Aopts%3Atest:foo', 'bar', function(err) {
            t.equal(err, null);
            space.del('foo', function(err) {
                t.equal(err, null);
                db.get('del%3Aopts%3Atest:foo', function(err, value) {
                    t.equal(value, undefined);
                    t.type(err, Error);
                    if (err) t.equal(err.name, 'NotFoundError');
                    t.end();
                });
            })
        });
    });

    t.test('batch', function(t) {
        var space = db.namespace('batch:test');
        var input = [
            { type: 'put', key: 'hello', value: 'world' },
            { type: 'put', key: 'foo', value: 'bar' }
        ];
        space.batch(input, function(err) {
            t.equal(err, null);
            t.equal(input[0].key, 'hello');
            t.equal(input[1].key, 'foo');
            db.get('batch%3Atest:hello', function(err, value) {
                t.equal(err, null);
                t.equal(value, 'world');
                t.end();
            });
        });
    });

    t.test('batch with opts', function(t) {
        var space = db.namespace('batch:opts:test');
        var input = [
            { type: 'put', key: 'hello', value: 'world' },
            { type: 'put', key: 'foo', value: true }
        ];
        var opts = { valueEncoding: 'json' }
        space.batch(input, opts, function(err) {
            t.equal(err, null);
            t.equal(input[0].key, 'hello');
            t.equal(input[1].key, 'foo');
            db.get('batch%3Aopts%3Atest:foo', opts, function(err, value) {
                t.equal(err, null);
                t.equal(value, true);
                t.end();
            });
        });

    });

    t.test('readable streams', function(t) {
        t.test('setup', function(t) {
            db.batch([
                { type: 'put', key: 'foo', value: 'baz' },
                { type: 'put', key: 'read%3Astream%3Atest:a', value: 'b' },
                { type: 'put', key: 'read%3Astream%3Atest:foo', value: 'bar' },
                { type: 'put', key: 'read%3Astream%3Atest:hello', value: 'world' },
                { type: 'put', key: 'read%3Astream%3Atest;hello', value: 'worldz' },
                { type: 'put', key: 'read%3Astream%3Atest;', value: 'baz' }
            ], function(err) {
                t.equal(err, null);
                t.end();
            });
        });

        var space = db.namespace('read:stream:test');

        t.test('readStream', function(t) {
            space.readStream().pipe(createArrayStream()).once('end', function(array) {
                t.deepEqual(array, [
                    { key : 'a', value : 'b' },
                    { key: 'foo', value: 'bar' },
                    { key: 'hello', value: 'world' }
                ]);
                t.end();
            });
        });

        t.test('readStream with start', function(t) {
            space.readStream({ start: 'foo' }).pipe(createArrayStream()).once('end', function(array) {
                t.deepEqual(array, [
                    { key: 'foo', value: 'bar' },
                    { key: 'hello', value: 'world' }
                ]);
                t.end();
            });
        });

        t.test('readStream with end', function(t) {
            space.readStream({ end: 'hell' }).pipe(createArrayStream()).once('end', function(array) {
                t.deepEqual(array[
                    { key: 'a', value : 'b' },                    
                    { key: 'foo', value: 'bar' }
                ]);
                t.end();
            });
        });

        t.test('readStream with keys === false', function(t) {
            space.readStream({ keys: false }).pipe(createArrayStream()).once('end', function(array) {
                t.deepEqual(array, [ 'b', 'bar', 'world' ]);
                t.end();
            });
        });

        t.test('readStream with values === false', function(t) {
            space.readStream({ values: false }).pipe(createArrayStream()).once('end', function(array) {
                t.deepEqual(array, [ 'a', 'foo', 'hello' ]);
                t.end();
            });
        });

        t.test('readStream with limit', function(t) {
            space.readStream({ limit: 2}).pipe(createArrayStream()).once('end', function(array) {
                t.deepEqual(array, [
                    { key: 'a', value : 'b' },                    
                    { key: 'foo', value: 'bar' }
                ]);
                t.end();
            });
        });

        t.test('keyStream', function(t) {
            space.keyStream().pipe(createArrayStream()).once('end', function(array) {
                t.deepEqual(array, [ 'a', 'foo', 'hello' ]);
                t.end();
            });
        });
        t.test('valueStream', function(t) {
            space.valueStream().pipe(createArrayStream()).once('end', function(array) {
                t.deepEqual(array, [ 'b', 'bar', 'world' ]);
                t.end();
            });
        });

        t.end();
    });

    t.test('writable streams', function(t) {
        t.plan(4);
        var space = db.namespace('write:stream:test');
        var stream = space.writeStream();
        stream.once('close', function() {
            setTimeout(function() {
                db.get('write%3Astream%3Atest:hello', function(err, value) {
                    t.equal(err, null);
                    t.equal(value, 'world');
                });
                db.get('write%3Astream%3Atest:foo', function(err, value) {
                    t.equal(err, null);
                    t.equal(value, 'bar');
                });
            }, 100);
        });
        stream.write({ key: 'hello', value: 'world' })
        stream.write({ key: 'foo', value: 'bar'})
        stream.end()
    });
});