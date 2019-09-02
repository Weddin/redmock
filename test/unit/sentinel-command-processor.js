'use strict';

const SentinelCommandProcessor = require('../../src/sentinel-command-processor');
const stream = require('stream');

require('../common');

describe('SentinelCommandProcessor', () => {

  let sentinelCommandProcessor;

  beforeEach(() => {
    sentinelCommandProcessor = new SentinelCommandProcessor(6379);
  });

  afterEach(() => {
  });

  describe('#process()', () => {

    it('should fail due to uknown command', (done) => {
      let msg = {
        type: '*',
        length: 2,
        value: [
          {
            type: '$',
            length: 8,
            value: 'sentinel'
          },
          {
            type: '$',
            length: 3,
            value: 'bad'
          }
        ]
      };
      let socket = new stream.PassThrough();
      socket.on('data', (data) => {
        data.toString().should.equal('-ERR unknown command\r\n');
        done();
      });
      sentinelCommandProcessor.process(msg, socket);
    });

    it('should process get-master-addr-by-name', (done) => {
      let msg = {
        type: '*',
        length: 3,
        value: [
          {
            type: '$',
            length: 8,
            value: 'sentinel'
          },
          {
            type: '$',
            length: 23,
            value: 'get-master-addr-by-name'
          },
          {
            type: '$',
            length: 8,
            value: 'mymaster'
          },
        ]
      };
      let socket = new stream.PassThrough();
      socket.on('data', (data) => {
        data.toString().should.equal('*2\r\n$9\r\n127.0.0.1\r\n$4\r\n6379\r\n');
        done();
      });
      sentinelCommandProcessor.process(msg, socket);
    });

  });

});
