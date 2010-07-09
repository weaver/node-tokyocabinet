var tc = require('tokyocabinet');

exports.HashDB = HashDB;

function HashDB() {
    this.db = null;
}

HashDB.prototype.open = function(path, flag, k) {
  var self = this,
      db = this.db,
      flags;

  if (db) {
    k(null, this);
    return this;
  }

  flags = open_flags(tc.HDB, flag);
  if (flags === undefined) {
    k(new Error("open: unrecognized mode '" + flag + "'."));
    return this;
  }

  db = new tc.HDB();

  if (!db.setmutex()) {
    k(last_error('HashDB.open() failed to set its mutex', db));
    return this;
  }

  db.openAsync(path, flags, function(err) {
    if (err) k(last_error('HashDB.open()', db, err));
    else {
      self.db = db;
      k(null);
    }
  });

  return this;
};

HashDB.prototype.isOpen = function() {
  return this.db !== null;
};

HashDB.prototype.close = function(k) {
  if (this.isOpen()) {
    var self = this;
    this.db.closeAsync(function(err) {
      if (err) k(last_error('HashDB.close()', self.db, err));
      else {
	this.db = null;
	k(null);
      }
    });
  }

  return this;
};

HashDB.prototype.get = function(key, k) {
  var self = this;
  this.db.getAsync(key, function(err, val) {
    (!err || err == tc.HDB.ENOREC) ? k(null, val) :
      k(last_error('HashDB.get()', self.db, err));
  });
  return this;
};

HashDB.prototype.put = function(key, val, k) {
  var self = this;
  this.db.putAsync(key, val, function(err) {
    err ? k(last_error('HashDB.put()', self.db, err)) :
      k(null, key, val);
  });
  return this;
};

HashDB.prototype.out = function(key, k) {
  var self = this;
  this.db.outAsync(key, function(err) {
    err ? k(last_error('HashDB.put()', self.db, err)) :
      k(null);
  });
  return this;
};

HashDB.prototype.addint = function(key, delta, k) {
  var self = this;
  this.db.addintAsync(key, delta, function(err, result) {
    err ? k(last_error('HashDB.addint()', self.db, err)) :
      k(null, result);
  });
  return this;
};


/// --- Aux

function open_flags(cls, flag) {
  switch(flag) {
    case 'r': return cls.OREADER;
    case 'r+': return cls.OWRITER;
    case 'w':
    case 'w+': return cls.OTRUNC | cls.OCREAT | cls.OWRITER;
    case 'a':
    case 'a+': return cls.OCREAT | cls.OWRITER;
    default: return undefined;
  }
}

function last_error(label, db, err) {
  return new Error(label + ': ' + db.errmsg(err));
}