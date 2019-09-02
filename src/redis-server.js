'use strict';

const net = require('net');
const MessageParser = require('./message-parser');
const CommandProcessor = require('./command-processor');
const SentinelCommandProcessor = require('./sentinel-command-processor');

const debug = require('debug')('redmock:redis-server');

module.exports = class RedisServer
{
    constructor(opts)
    {
        opts = {
            port: 6379,
            sentinelPort: 26379,
            ...opts
        };

        this.opts = opts;
        this.messageParser = new MessageParser();
        this.commandProcessor = new CommandProcessor();
        this.sentinelCommandProcessor = new SentinelCommandProcessor(this.opts.port);
        this.connections = [ ];
        this.sentinelConnections = [ ];
    }

    start()
    {
        return new Promise((resolve, reject) =>
        {
            this.server = net.createServer(this.handleNewConnection.bind(this));

            // Handle an error from listen
            this.server.once('error', (err) =>
            {
                this.server.removeAllListeners('error');
                reject(err);
            });

            // We are listening
            this.server.once('listening', () =>
            {
                this.server.removeAllListeners('error');

                // Now let's start a sentinel server
                this.sentinelServer = net.createServer(this.handleSentinelConnection.bind(this));

                // Handle an error from listen
                this.sentinelServer.once('error', (err) =>
                {
                    this.sentinelServer.removeAllListeners('error');
                    reject(err);
                });

                // We are listening
                this.sentinelServer.once('listening', () =>
                {
                    this.sentinelServer.removeAllListeners('error');
                    debug('Server started');
                    resolve(true);
                });

                this.sentinelServer.listen(this.opts.sentinelPort);
            });

            this.server.listen(this.opts.port);
        });
    }

    stop()
    {
        return new Promise((resolve) =>
        {
            if(this.server)
            {
                debug('Stopping server');
                for(const connection of this.connections)
                {
                    connection.destroy();
                }
                for(const connection of this.sentinelConnections)
                {
                    connection.destroy();
                }
                this.server.close(() =>
                {
                    if(this.sentinelServer)
                    {
                        this.sentinelServer.close(() =>
                        {
                            resolve(true);
                        });
                    }
                    else
                    {
                        resolve(true);
                    }
                });
            }
            else
            {
                resolve(true);
            }
        });
    }

    handleNewConnection(socket)
    {
        debug(`New redis connection from: ${ socket.remoteAddress }:${ socket.remotePort }`);
        socket.database = '0';
        this.connections.push(socket);
        socket.on('data', (data) =>
        {
            const commands = this.messageParser.getCommands(data);
            for(const command of commands)
            {
                debug('Command is', command);
                this.commandProcessor.process(this.messageParser.parse(command), socket);
            }
        });
    }

    handleSentinelConnection(socket)
    {
        debug(`New sentinel connection from: ${ socket.remoteAddress }:${ socket.remotePort }`);
        this.sentinelConnections.push(socket);
        socket.on('data', (data) =>
        {
            this.sentinelCommandProcessor.process(this.messageParser.parse(data), socket);
        });
    }
};
