'use strict';

const Database = require('../../src/database');

require('../common');

describe('Database', () => {

  let database;

  beforeEach(() => {
    database = new Database();
  });

  describe('#createDatabase()', () => {

    it('should create new database', () => {
      database.createDatabase('1');
      should.exist(database.data['1']);
    });

    it('should not override existing databae', () => {
      database.data['1'] = {
        good: {
          value: 'good',
          ttl: 86400
        }
      }
      database.createDatabase('1');
      should.exist(database.data['1']);
      should.exist(database.data['1'].good);
    });

  });

  describe('#getIfNotExpired()', () => {

    it('should fail to due expired', () => {
      let now = Date.now();
      let created = new Date(now - (20 * 1000));
      database.data['0'] = {
        good: {
          value: 'good',
          ttl: 10,
          created: created
        }
      }
      let res = database.getIfNotExpired('good', '0');
      should.not.exist(res);
    });

    it('should succeed with default ttl', () => {
      database.data['0'] = {
        good: {
          value: 'good',
          ttl: -1,
          created: new Date()
        }
      }
      let res = database.getIfNotExpired('good', '0');
      should.exist(res);
    });

    it('should succeed with tll set', () => {
      database.data['0'] = {
        good: {
          value: 'good',
          ttl: 86400,
          created: new Date()
        }
      }
      let res = database.getIfNotExpired('good', '0');
      should.exist(res);
    });

  });

  describe('#get()', () => {

    it('should fail due to uknown key', () => {
      let res = database.get('bad', '0');
      should.not.exist(res);
    });

    it('should fail due to uknown db', () => {
      let res = database.get('bad', '1');
      should.not.exist(res);
    });

    it('should succeed', () => {
      database.data['0'].good = {
        value: 'good',
        ttl: 86400,
        created: new Date()
      };
      let res = database.get('good', '0');
      should.exist(res);
      res.value.should.equal('good');
      res.ttl.should.equal(86400);
    });

  });

  describe('#set()', () => {

    it('should fail due to uknown db', () => {
      let res = database.set('good', 'good', '1');
      res.should.equal(false);
    });

    it('should set with default opts', () => {
      let res = database.set('good', 'good', '0');
      res.should.equal(true);
      database.data['0'].good.value.should.equal('good');
    });

    it('should fail due to invalid options', () => {
      let res = database.set('good', 'good', '0', { exists: true, notExists: true });
      res.should.equal(false);
    });

    it('should succeed with notExists true', () => {
      let res = database.set('good', 'good', '0', { notExists: true });
      res.should.equal(true);
      database.data['0'].good.value.should.equal('good');
    });

    it('should fail with noExists true', () => {
      database.data['0'].good = {
        value: 'good'
      };
      let res = database.set('good', 'good', '0', { notExists: true });
      res.should.equal(false);
    });

    it('should succeed with exists true', () => {
      database.data['0'].good = {
        value: 'good'
      };
      let res = database.set('good', 'good', '0', { exists: true });
      res.should.equal(true);
    });

    it('should fail with exists true', () => {
      let res = database.set('good', 'good', '0', { exists: true });
      res.should.equal(false);
    });

  });

});
