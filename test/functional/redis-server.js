const Redis = require('ioredis');
const RedisServer = require('../../index');

describe('RedisServer', () => {

  let redis, redisServer;

  describe('connect', () => {

    it('should connect to standalone', (done) => {
      redisServer = new RedisServer();
      redisServer.start().then((res) => {
        redis = new Redis();
        redis.on('connect', () => {
          redis.disconnect();
          redisServer.stop().then((res) => {
            done();
          });
        });
      }).catch((err) => {
        done(err);
      });
    });

    it('should connect via sentinel', (done) => {
      redisServer = new RedisServer();
      redisServer.start().then((res) => {
        redis = new Redis({
          sentinels: [{ host: 'localhost', port: 26379 }],
          name: 'mymaster'
        });
        redis.on('connect', () => {
          redis.disconnect();
          redisServer.stop().then((res) => {
            done();
          });
        });
      }).catch((err) => {
        done(err);
      });
    });

  });

  describe('INFO', () => {

    before((done) => {
      redisServer = new RedisServer();
      redisServer.start().then((res) => {
        redis = new Redis();
        redis.on('connect', () => {
          done();
        });
      }).catch((err) => {
        done(err);
      });
    });

    after((done) => {
      redis.disconnect();
      redisServer.stop().then((res) => {
        done();
      });
    });

    it('should get server info', () => {
      return redis.info().should.eventually.equal('# Server\r\nredis_version:3.0.0\r\n# Clients\r\n# Memory\r\n# Persistence\r\n# Stats\r\n# Replication\r\n# CPU\r\n# Cluster\r\n# Keyspace\r\ndb0:keys=1997,expires=1,avg_ttl=98633637897');
    });

  });

  describe('SELECT', () => {

    before((done) => {
      redisServer = new RedisServer();
      redisServer.start().then((res) => {
        redis = new Redis();
        redis.on('connect', () => {
          done();
        });
      }).catch((err) => {
        done(err);
      });
    });

    after((done) => {
      redis.disconnect();
      redisServer.stop().then((res) => {
        done();
      });
    });

    it('should select a new db', () => {
      return redis.select('1').should.eventually.equal('OK');
    });

  });

  describe('GET/SET', () => {

    before((done) => {
      redisServer = new RedisServer();
      redisServer.start().then((res) => {
        redis = new Redis();
        redis.on('connect', () => {
          done();
        });
      }).catch((err) => {
        done(err);
      });
    });

    after((done) => {
      redis.disconnect();
      redisServer.stop().then((res) => {
        done();
      });
    });

    it('should fail to get non-existent key', () => {
      return redis.get('nonexistent').should.eventually.equal(null);
    });

    it('should set with no expiration', () => {
      return redis.set('noexp', 'noexp').should.eventually.equal('OK');
    });

    it('should get key with no expiration', () => {
      return redis.get('noexp').should.eventually.equal('noexp');
    });

    it('should set key with expiration in ms', () => {
      return redis.set('expinms', 'expinms', 'PX', 1).should.eventually.equal('OK');
    });

    it('should set key with expiration in seconds', () => {
      return redis.set('expinsecs', 'expinsecs', 'EX', 600).should.eventually.equal('OK');
    });

    it('should get non expired key set with secs', () => {
      return redis.get('expinsecs').should.eventually.equal('expinsecs');
    });

    it('should fail to get expired key set with ms', (done) => {
      setTimeout(() => {
        redis.get('expinms').then((res) => {
          should.not.exist(res);
          done();
        }).catch((err) => {
          done(err);
        });
      }, 10);
    });

    it('should set with not exists', () => {
      return redis.set('notexists', 'notexists', 'NX').should.eventually.equal('OK');
    });

    it('should fail to set with not exists', () => {
      return redis.set('notexists', 'notexists', 'NX').should.eventually.equal(null);
    });

    it('should set with exists', () => {
      return redis.set('notexists', 'notexists', 'XX').should.eventually.equal('OK');
    });

    it('should fail to set with exists', () => {
      return redis.set('exists', 'exists', 'XX').should.eventually.equal(null);
    });

    it('should support setex', () => {
      return redis.setex('foo', 'bar', 500).should.eventually.equal('OK');
    });
  });

  describe('DEL', () => {

    before((done) => {
      redisServer = new RedisServer();
      redisServer.start().then((res) => {
        redis = new Redis();
        redis.on('connect', () => {
          done();
        });
      }).catch((err) => {
        done(err);
      });
    });

    after((done) => {
      redis.disconnect();
      redisServer.stop().then((res) => {
        done();
      });
    });

    it('should delete a single key', () => {
      return redis.del('foo').should.eventually.equal(0);
    });

    it('should delete many keys', () => {
      return redis.del('foo', 'bar', 'baz').should.eventually.equal(0);
    });
  });
});
