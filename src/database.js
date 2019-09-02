'use strict';

const debug = require('debug')('redmock:database');
const error = require('debug')('redmock:error');

/**
 * Simple database to store values.
 */
module.exports = class Database {

  /**
   * Constructor.
   * @constructor
   */
  constructor() {
    error.color = 1;
    this.data = {
      '0': {
      }
    };
  }

  /**
   * Create a new db.
   */
  createDatabase(db) {
    if (!this.data[db]) {
      debug('Create new database ' + db);
      this.data[db] = {
      };
    }
  }

  /**
   * Get a value.
   */
  get(key, db) {
    debug('Get ' + key + ' from ' + db);

    if (!this.data[db]) {
      error('Unknown database: ' + db);
      return null;
    } else {
      if (this.data[db][key]) {
        return this.getIfNotExpired(key, db);
      } else {
        error('Unkown key ' + key + ' in ' + db);
        return null;
      }
    }
  }

  /**
   * Get if not expired.
   */
  getIfNotExpired(key, db) {
    let data = this.data[db][key];
    if (data.ttl > 0) {
      let now = Date.now();
      let then = data.created.getTime();
      let elapsed = now - then;
      let ttl = data.ttl * 1000;
      debug('TTL in ms ' + ttl);
      debug(elapsed + ' ms have elapsed since created');
      if (elapsed > ttl) {
        debug('Data has expired');
        delete this.data[db][key];
        return null;
      } else {
        return data;
      }
    } else {
      return data;
    }
  }

  /**
   * Set a value.
   */
  set(key, value, db, opts) {
    opts = opts || {
      expiresIn: -1,
      notExists: false,
      exists: false
    };
    debug('Set ' + key + ' in ' + db + ' with opts %j', opts);

    if (!this.data[db]) {
      error('Unknown database: ' + db);
      return false;
    } else {
      let data = {
        value: value,
        ttl: opts.expiresIn,
        created: new Date()
      };
      // Always set if notExists or exist is false
      if (!opts.notExists && !opts.exists) {
        debug('Setting ' + key + ' in ' + db + ' to %j', data);
        this.data[db][key] = data;
        return true;
      } else if (opts.notExists && !opts.exists) {
        if (!this.data[db][key]) {
          debug('Setting ' + key + ' in ' + db + ' to %j', data);
          this.data[db][key] = data;
          return true;
        } else {
          error('Not exists set to true, but value already exists');
          return false;
        }
      } else if (!opts.notExists && opts.exists) {
        if (!this.data[db][key]){
          error('Exists set to true, but value does not exist');
          return false;
        } else {
          debug('Setting ' + key + ' in ' + db + ' to %j', data);
          this.data[db][key] = data;
          return true;
        }
      } else {
        error('Can not set both exists and not exists at the same time');
        return false;
      }
    }
  }

  /**
   * Delete a value
   */
  del(key, db) {
    if (!this.data[db]) {
      error('Unknown database: ' + db);
      return null;
    } else {
      if (this.data[db][key]) {
        delete this.data[db][key];
        return true;
      } else {
        return false;
      }
    }
  }
}
