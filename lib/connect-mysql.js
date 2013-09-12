module.exports = function (connect) {
  var Store = connect.session.Store;

  function MySQLStore(options) {
    var self = this;
    self.cleanup = true;
    self.sqlCleanup = true;
    Store.call(self, options);

    // cleanup at random
    if (options.hasOwnProperty('cleanup')) {
      self.cleanup = options.cleanup;
    }

    // cleanup by SQL event
    if (options.hasOwnProperty('sqlCleanup')) {
      self.cleanup = false;
      self.sqlCleanup = options.sqlCleanup;
    }

    self.mysql = options.client;
    self.mysql.query('CREATE TABLE IF NOT EXISTS `Session` (`sid` varchar(255) NOT NULL, `session` text NOT NULL, `expires` int(11) DEFAULT NULL, `user` int(11) DEFAULT NULL, `created` timestamp NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (`sid`), KEY `idx_se_expires` (`expires`), KEY `idx_se_user` (`user`))',
      function dbCreated(err) {
        if (err) throw err;

        // cleanup by SQL event
        if (self.sqlCleanup) {
          self.mysql.query('CREATE EVENT IF NOT EXISTS `sess_cleanup` ON SCHEDULE EVERY 15 MINUTE DO DELETE FROM `Session` WHERE `expires` < UNIX_TIMESTAMP()');
          self.mysql.query('SET GLOBAL event_scheduler = 1');
        }

      });
  }

  function cleanup() {
    self.mysql.query('DELETE FROM `Session` WHERE `expires` < UNIX_TIMESTAMP()', function dbCleaned(err) {
      if (err) {
        throw err;
      }
    });
  }

  MySQLStore.prototype.__proto__ = Store.prototype;

  MySQLStore.prototype.get = function (sid, callback) {
    this.mysql.query('SELECT `session` FROM `Session` WHERE `sid` = ?', [sid],function (err, result) {
      if (result && result[0] && result[0].session) {
        callback(null, JSON.parse(result[0].session));
      } else {
        callback(err);
      }
    }).on('error', function (err) {
        callback(err);
      });
  };

  MySQLStore.prototype.set = function (sid, session, callback) {
    var expires = new Date(session.cookie.expires).getTime() / 1000,
      user = null;

    // cleanup at random
    if (this.cleanup && Math.floor(Math.random() * 6) === 5) {
      cleanup();
    }

    if (session.passport.user) {
      user = session.passport.user;
    }

    session = JSON.stringify(session);
    this.mysql.query('INSERT INTO `Session` (`sid`, `session`, `expires`, `user`) VALUES(?, ?, ?, ?) ON DUPLICATE KEY UPDATE `session` = ?, `expires` = ?, `user` = ?',
      [sid, session, expires, user, session, expires, user], function sessionInserted(err) {
      callback(err);
    });
  };

  MySQLStore.prototype.destroy = function (sid, callback) {
    this.mysql.query('DELETE FROM `Session` WHERE `sid` = ?', [sid], function sessionDestroyed(err) {
      if(callback) {
        callback(err);
      }
    });
  };

  return MySQLStore;
};
