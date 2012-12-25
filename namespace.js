var through = require('through');

var NamespaceDB = function(db, name) {
    this.db = db;
    this.name = name;
};

function isObj(value) {
    return typeof(value) === 'object' && value !== null;
}
function copy(obj) {
    var start = Array.isArray(obj)? [] : {};
    return Object.keys(obj).reduce(function(newObj, key) {
        var value = obj[key];
        if (isObj(value)) {
            value = copy(value);
        }
        newObj[key] = value;
        return newObj;
    }, start);
}

NamespaceDB.prototype._callFunc = function(name, args) {
    // replace the key with the namespaced key
    args[0] = this.name + ':' + args[0];
    this.db[name].apply(this.db, args);
}

NamespaceDB.prototype.put = function() {
    this._callFunc('put', arguments);
}

NamespaceDB.prototype.get = function() {
    this._callFunc('get', arguments);
}

NamespaceDB.prototype.del = function() {
    this._callFunc('del', arguments);
}

NamespaceDB.prototype.batch = function() {
    var self = this;
    batch = copy(arguments[0]);
    batch.forEach(function(row) {
        row.key = self.name + ':' + row.key;
    });
    arguments[0] = batch;
    this.db.batch.apply(this.db, arguments);
}

NamespaceDB.prototype.keyStream = function(_opts) {
    var opts = copy(_opts || {});
    opts.keys = true;
    opts.values = false;
    return this.readStream(opts);
}

NamespaceDB.prototype.valueStream = function(_opts) {
    var opts = copy(_opts || {});
    opts.keys = false;
    opts.values = true;
    return this.readStream(opts);
}

NamespaceDB.prototype.readStream = function(opts) {
    var opts = copy(opts || {});

    opts.start = this.name + ':' + (opts.start? opts.start : '');

    var self = this;
    var keys = opts.keys === undefined ? true : opts.keys;
    if (opts.values === undefined) opts.values = true;
    // need key to filter on
    opts.keys = true;

    var readStream = this.db.readStream(opts);
    var stream = through(function write(row) {
        var key = row.key || row;
        if (key.slice(0, self.name.length + 1) !== self.name + ':') {
            readStream.destroy();
            this.queue(null);
            return;
        }
 
        if (opts.values) {
            if (!keys) {
                row = row.value;
            } else {
                row.key = row.key.slice(self.name.length + 1);
            }
        } else {
            row = row.slice(self.name.length + 1);
        }
        this.queue(row);
    });
    readStream.pipe(stream);
    return stream;
}

NamespaceDB.prototype.writeStream = function() {
    var self = this;
    var stream = through(function write(row) {
        row.key = self.name + ':' + row.key;
        this.queue(row);
    });
    stream.pipe(this.db.writeStream());
    return stream;
}

module.exports = function(db) {
    db.namespace = function(name) {
        return new NamespaceDB(db,name);
    }
}