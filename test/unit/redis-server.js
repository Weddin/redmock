'use strict';

const RedisServer = require('../../src/redis-server');
const stream = require('stream');
const sinon = require('sinon');

require('../common');

describe('RedisServer', () =>
{

    let redisServer, processSentinelCommand, processCommand;

    beforeEach(() =>
    {
        redisServer = new RedisServer();
        processSentinelCommand = sinon.stub(redisServer.sentinelCommandProcessor, 'process').callsFake((msg, socket) =>
        {
        });
        processCommand = sinon.stub(redisServer.commandProcessor, 'process').callsFake((msg, socket) =>
        {
        });
    });

    afterEach((done) =>
    {
        processSentinelCommand.restore();
        processCommand.restore();
        redisServer.stop().then((res) =>
        {
            done();
        });
    });

    describe('#constructor()', () =>
    {

        it('should construct new instance with default opts', () =>
        {
            let instance = new RedisServer();
            instance.opts.port.should.equal(6379);
            instance.opts.sentinelPort.should.equal(26379);
        });

        it('should construct new instance with specific opts', () =>
        {
            let instance = new RedisServer({ port: 666, sentinelPort: 777 });
            instance.opts.port.should.equal(666);
            instance.opts.sentinelPort.should.equal(777);
        });

    });

    describe('#start()', () =>
    {

        xit('should fail to start due to listen error', () =>
        {
            // This should fail because a normal user can not listen on low
            // numbered ports.
            redisServer.opts.port = 1;
            return redisServer.start().should.be.rejectedWith(/EACCES/);
        });

        xit('should fail to start due to sentinel listen error', () =>
        {
            // This should fail because a normal user can not listen on low
            // numbered ports.
            redisServer.opts.sentinelPort = 1;
            return redisServer.start().should.be.rejectedWith(/EACCES/);
        });

        it('should succeed', (done) =>
        {
            redisServer.start().then((res) =>
            {
                redisServer.server.listening.should.equal(true);
                redisServer.sentinelServer.listening.should.equal(true);
                done();
            }).catch((err) =>
            {
                done(err);
            });
        });

    });

    describe('#stop()', () =>
    {

        beforeEach((done) =>
        {
            redisServer.start().then((res) =>
            {
                redisServer.connections.push({ destroy: function () {} });
                redisServer.sentinelConnections.push({ destroy: function () {} });
                done();
            });
        });

        it('should succeed', () =>
        {
            return redisServer.stop().should.be.fulfilled;
        });

    });

    describe('#handleNewConnection()', () =>
    {

        it('should handle connection', () =>
        {
            let socket = new stream.PassThrough();
            redisServer.handleNewConnection(socket);
            socket.write('data');
        });

    });

    describe('#handleSentinelConnection()', () =>
    {

        it('should handle connection', () =>
        {
            let socket = new stream.PassThrough();
            redisServer.handleSentinelConnection(socket);
            socket.write('data');
        });

    });

});
