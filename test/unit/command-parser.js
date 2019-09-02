'use strict';

const CommandProcessor = require('../../src/command-processor');
const stream = require('stream');

require('../common');

describe('CommandProcessor', () => {

  let commandProcessor;

  beforeEach(() => {
    commandProcessor = new CommandProcessor();
  });

  afterEach(() => {
  });

  describe('#process()', () => {

    it('should fail due to uknown command', (done) => {
      let msg = {
        type: '*',
        length: 1,
        value: [
          {
            type: '$',
            length: 3,
            value: 'bad'
          },
        ]
      };
      let socket = new stream.PassThrough();
      socket.on('data', (data) => {
        data.toString().should.equal('-ERR unknown command\r\n');
        done();
      });
      commandProcessor.process(msg, socket);
    });

    it('should process select', (done) => {
      let msg = {
        type: '*',
        length: 2,
        value: [
          {
            type: '$',
            length: 6,
            value: 'select'
          },
          {
            type: '$',
            length: 1,
            value: '1'
          }
        ]
      };
      let socket = new stream.PassThrough();
      socket.database = '0';
      socket.on('data', (data) => {
        data.toString().should.equal('+OK\r\n');
        socket.database.should.equal('1');
        done();
      });
      commandProcessor.process(msg, socket);
    });

    it('should process info', (done) => {
      let msg = {
        type: '*',
        length: 1,
        value: [
          {
            type: '$',
            length: 4,
            value: 'info'
          },
        ]
      };
      let socket = new stream.PassThrough();
      socket.on('data', (data) => {
        data.toString().should.equal('$164\r\n# Server\r\nredis_version:3.0.0\r\n# Clients\r\n# Memory\r\n# Persistence\r\n# Stats\r\n# Replication\r\n# CPU\r\n# Cluster\r\n# Keyspace\r\ndb0:keys=1997,expires=1,avg_ttl=98633637897\r\n');
        done();
      });
      commandProcessor.process(msg, socket);
    });

    it('should process set', (done) => {
      let msg = {
        type: '*',
        length: 3,
        value: [
          {
            type: '$',
            length: 3,
            value: 'set'
          },
          {
            type: '$',
            length: 3,
            value: 'foo'
          },
          {
            type: '$',
            length: 3,
            value: 'bar'
          }
        ]
      };
      let socket = new stream.PassThrough();
      socket.database = '0';
      socket.on('data', (data) => {
        data.toString().should.equal('+OK\r\n');
        done();
      });
      commandProcessor.process(msg, socket);
    });

    it('should process set with bad option and exp in ms', (done) => {
      let msg = {
        type: '*',
        length: 3,
        value: [
          {
            type: '$',
            length: 3,
            value: 'set'
          },
          {
            type: '$',
            length: 3,
            value: 'foo'
          },
          {
            type: '$',
            length: 3,
            value: 'bar'
          },
          {
            type: '$',
            length: 2,
            value: 'FF'
          },
          {
            type: '$',
            length: 2,
            value: 'PX'
          },
          {
            type: '$',
            length: 3,
            value: '100000'
          }
        ]
      };
      let socket = new stream.PassThrough();
      socket.database = '0';
      socket.on('data', (data) => {
        data.toString().should.equal('+OK\r\n');
        done();
      });
      commandProcessor.process(msg, socket);
    });

    it('should process set with NX (exp in seconds)', (done) => {
      let msg = {
        type: '*',
        length: 3,
        value: [
          {
            type: '$',
            length: 3,
            value: 'set'
          },
          {
            type: '$',
            length: 3,
            value: 'foo'
          },
          {
            type: '$',
            length: 3,
            value: 'bar'
          },
          {
            type: '$',
            length: 2,
            value: 'NX'
          },
          {
            type: '$',
            length: 2,
            value: 'EX'
          },
          {
            type: '$',
            length: 3,
            value: '100'
          }
        ]
      };
      let socket = new stream.PassThrough();
      socket.database = '0';
      socket.on('data', (data) => {
        data.toString().should.equal('+OK\r\n');
        done();
      });
      commandProcessor.process(msg, socket);
    });

    it('should fail to set with exists set to true', (done) => {
      let msg = {
        type: '*',
        length: 3,
        value: [
          {
            type: '$',
            length: 3,
            value: 'set'
          },
          {
            type: '$',
            length: 3,
            value: 'foo'
          },
          {
            type: '$',
            length: 3,
            value: 'bar'
          },
          {
            type: '$',
            length: 2,
            value: 'XX'
          }
        ]
      };
      let socket = new stream.PassThrough();
      socket.database = '0';
      socket.on('data', (data) => {
        data.toString().should.equal('$-1\r\n');
        done();
      });
      commandProcessor.process(msg, socket);
    });

    it('should process get', (done) => {
      let msg = {
        type: '*',
        length: 2,
        value: [
          {
            type: '$',
            length: 3,
            value: 'get'
          },
          {
            type: '$',
            length: 3,
            value: 'foo'
          }
        ]
      };
      commandProcessor.database.data['0'].foo = {
        value: 'bar',
        ttl: 86400,
        created: new Date()
      };
      let socket = stream.PassThrough();
      socket.database = '0';
      socket.on('data', (data) => {
        data.toString().should.equal('$3\r\nbar\r\n');
        done();
      });
      commandProcessor.process(msg, socket);
    });

    it('should fail to get', (done) => {
      let msg = {
        type: '*',
        length: 2,
        value: [
          {
            type: '$',
            length: 3,
            value: 'get'
          },
          {
            type: '$',
            length: 3,
            value: 'foo'
          }
        ]
      };
      let socket = stream.PassThrough();
      socket.database = '0';
      socket.on('data', (data) => {
        data.toString().should.equal('$-1\r\n');
        done();
      });
      commandProcessor.process(msg, socket);
    });

  });

});
