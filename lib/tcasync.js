var tc = require('./tokyocabinet');

exports.HashDB = HashDB;

function HashDB() {
    this.db = null;
}


/// --- Open and Close

HashDB.prototype.open = function(path, flag, k) {
  var self = this,
      db = this.db,
      flags;

  if (db) {
    k(null);
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
      self.path = path;
      k(null);
    }
  });

  return this;
};

HashDB.prototype.isOpen = function() {
  return this.db !== null;
};

HashDB.prototype.close = function(k) {
  if (!this.isOpen())
    k(null);
  else {
    var self = this;
    this.db.closeAsync(function(err) {
      if (err) k(last_error('HashDB.close()', self.db, err));
      else {
        self.db = null;
        self.path = null;
        k(null);
      }
    });
  }

  return this;
};

HashDB.prototype.destroy = function(k) {
  if (!this.path)
    k(null);
  else {
    var path = this.path;
    this.close(function(err) {
      err ? k(err) : fs.unlink(path, k);
    });
  }

  return this;
};


/// --- Data

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

HashDB.prototype.putcat = function(key, val, k) {
  this.db.putcatAsync(key, val, function(err) {
    err ? k(last_error('HashDB.putcat()', self.db, err)) :
      k(null, key, val);
  });
};

HashDB.prototype.out = function(key, k) {
  var self = this;
  this.db.outAsync(key, function(err) {
    err ? k(last_error('HashDB.out()', self.db, err)) :
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

HashDB.prototype.keys = function(fn, k) {
  var self = this;

  k = k || function(err) { if (err) throw err; };
  this.db.iterinitAsync(function(err) {
    err ? abort(err) : step();
  });

  function abort(err) {
    k(last_error('HashDB.keys()', self.db, err));
  }

  function step(err) {
    if (err)
      k(err);
    else
      self.db.iternextAsync(function(err, key) {
        if (err && err != tc.HDB.ENOREC)
          abort(err);
        else if (!key)
          k(null);
        else
          fn(key, step);
      });
  }

  return this;
};

HashDB.prototype.items = function(fn, k) {
  var self = this;
  return this.keys(function(key, next) {
    self.get(key, function(err, val) {
      err ? next(err) : fn(key, val, next);
    });
  }, k);
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