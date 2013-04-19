#DEPRECATED

Yo! [level-sublevel](https://github.com/dominictarr/level-sublevel) is much more capable than this, so use that instead!

# level-namespace[![build status](https://secure.travis-ci.org/kesla/level-namespace.png)](http://travis-ci.org/kesla/level-namespace)
namespaces for levelup

## Usage/demo

```javascript

var namespace = require('level-namespace');
namespace(db);

var namespace = db.namespace('example');

// most of the levelup-methods are supported
namespace.put('foo', 'bar', function(err) {
    if (err) throw err;

    namespace.get('foo', function(err, value) {
        assert.equal(value, 'bar');
    });

    db.get('example:foo', function(err, value) {
        assert.equal(value, 'bar');
    });
});

db.batch([
        { type: 'put', key: 'foo', value: 'baz' },
        { type: 'put', key: 'example:foo', value: 'bar' },
        { type: 'put', key: 'example:hello', value: 'world' },
        { type: 'put', key: 'example;hello', value: 'worldz' }
    ], function() {
        // will print
        // 'bar' and then 'world' to process.stdout
        namespace.valueStream().pipe(process.stdout)
        // namespace also support valueStream, readStream & writeStream also
    }
)

```

## Licence
MIT
