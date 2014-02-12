'use strict';

var util = require('util');

module.exports = function (connect) {
  var
    MySQLStore,
    DELETE_FREQ = 20;


  /**
   * MySQLStore
   * extend/override connect.session.store
   *
   * @constructor
   * @method MySQLStore
   */
  MySQLStore = function (options) {
    var _this = this;

    _this.isCleanup = true;
    _this.isSqlCleanup = true;

    connect.session.Store.call(_this, options);

    // option: cleanup at random
    if (options.hasOwnProperty('cleanup')) {
      this.isCleanup = options.cleanup;
    }

    // option: cleanup by SQL event
    if (options.hasOwnProperty('sqlCleanup')) {
      _this.isCleanup = false;
      _this.isSqlCleanup = options.sqlCleanup;
    }

    // option: delete freq
    if(options.freq && !isNaN(options.freq)) {
      DELETE_FREQ = options.freq;
    }

    _this.mysql = options.client;
    _this.mysql.query('CREATE TABLE IF NOT EXISTS `Session` (`sid` varchar(255) NOT NULL, `session` text NOT NULL, `expires` int(11) DEFAULT NULL, `user` int(11) DEFAULT NULL, `created` timestamp NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (`sid`), KEY `idx_se_expires` (`expires`), KEY `idx_se_user` (`user`))', function (err) {
        if (err) {
          throw err;
        }

        // cleanup by SQL event
        if (_this.isSqlCleanup) {
          _this.mysql.query('CREATE EVENT IF NOT EXISTS `sess_cleanup` ON SCHEDULE EVERY 15 MINUTE DO DELETE FROM `Session` WHERE `expires` < UNIX_TIMESTAMP()');
          _this.mysql.query('SET GLOBAL event_scheduler = 1');
        }
      });
  };

  // Inherit Store from the connect.session.store
  util.inherits(MySQLStore, connect.session.Store);


  /**
   * Cleanup
   * delete expires sessions
   *
   * @method cleanup
   */
  MySQLStore.prototype.cleanup = function () {
    this.mysql.query('DELETE FROM `Session` WHERE `expires` < UNIX_TIMESTAMP()', function (err) {
      if (err) {
        throw err;
      }
    });
  };


  /**
   * Get
   * override connect session store function
   *
   * @method get
   * @param {String} sid
   * @param {Function} callback
   */
  MySQLStore.prototype.get = function (sid, callback) {

    // cleanup at random
    if (this.isCleanup && !this.isSqlCleanup && (Math.floor(Math.random() * (DELETE_FREQ + 1)) === DELETE_FREQ)) {
      this.cleanup();
    }

    this.mysql.query('SELECT `session` FROM `Session` WHERE `sid` = ?', [sid],function (err, result) {
      if (result && result[0] && result[0].session) {
        callback(null, JSON.parse(result[0].session));
      } else {
        callback(err);
      }
    }).on('error', function (err) {
        if (typeof callback === 'function') {
          callback(err);
        }
      });
  };


  /**
   * Get
   * override connect session store function
   *
   * @method get
   * @param {String} sid
   * @param {Object} session
   * @param {Function} callback
   */
  MySQLStore.prototype.set = function (sid, session, callback) {
    var expires = new Date(session.cookie.expires).getTime() / 1000,
      user = null;

    if (session.passport.user) {
      user = session.passport.user;
    }

    session = JSON.stringify(session);
    this.mysql.query('INSERT INTO `Session` (`sid`, `session`, `expires`, `user`) VALUES(?, ?, ?, ?) ON DUPLICATE KEY UPDATE `session` = ?, `expires` = ?, `user` = ?',
      [sid, session, expires, user, session, expires, user], function (err) {
        if (typeof callback === 'function') {
          callback(err);
        }
      });
  };


  /**
   * Destroy
   * override connect session store function
   *
   * @method destroy
   * @param {String} sid
   * @param {Function} callback
   */
  MySQLStore.prototype.destroy = function (sid, callback) {
    this.mysql.query('DELETE FROM `Session` WHERE `sid` = ?', [sid], function (err) {
      if (typeof callback === 'function') {
        callback(err);
      }
    });
  };

  return MySQLStore;
};
