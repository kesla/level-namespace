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

NamespaceDB.prototype.put = function(key, value, callback) {
    this.db.put(this.name + ':' + key, value, callback);
}

NamespaceDB.prototype.get = function(key, callback) {
    this.db.get(this.name + ':' + key, callback);
}

NamespaceDB.prototype.del = function(key, callback) {
    this.db.del(this.name + ':' + key, callback);
}

NamespaceDB.prototype.batch = function(batch, callback) {
    var self = this;
    batch = copy(batch);
    batch.forEach(function(row) {
        row.key = self.name + ':' + row.key;
    });
    this.db.batch(batch, callback);
}

NamespaceDB.prototype.keyStream = function() {
    var self = this;
    var opts = {
        start: this.name + ':',
        end: this.name + ';',
        keys: true,
        values: false
    }
    var stream = through(function write(row) {
        this.queue(row.slice(self.name.length + 1));
    });
    this.db.readStream(opts).pipe(stream);
    return stream;
}

NamespaceDB.prototype.valueStream = function() {
    var opts = {
        start: this.name + ':',
        end: this.name + ';',
        keys: false,
        values: true
    }
    // pipe to through so that the stream does pause and resume properly
    var stream = through();
    this.db.readStream(opts).pipe(stream);
    return stream;
}

NamespaceDB.prototype.readStream = function() {
    var self = this;
    var opts = {
        start: this.name + ':',
        end: this.name + ';',
        key: true,
        value: true
    };
    var stream = through(function write(row) {
        row.key = row.key.slice(self.name.length + 1)
        this.queue(row);
    });
    this.db.readStream(opts).pipe(stream);
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