'use strict';

const { EventEmitter } = require('events');
const MessageParser = require('./message-parser');
const Database = require('./database');

const debug = require('debug')('redmock:command-processor');
const error = require('debug')('redmock:error');

module.exports = class CommandProcessor extends EventEmitter
{
    constructor()
    {
        super();

        error.color = 1;
        this.messageParser = new MessageParser();
        this.database = new Database();
        this.blockedQueue = [];
    }

    process(msg, socket)
    {
        const commandType = this._getCommandType(msg);
        debug(`Process command type of ${ commandType } from ${ socket.remoteAddress }:${ socket.remotePort }`);
        debug(msg);
        switch (commandType)
        {
            case (CommandProcessor.INFO):
                this._processInfo(msg, socket);
                break;

            case (CommandProcessor.QUIT):
                this._processQuit(msg, socket);
                break;

            case (CommandProcessor.SET):
                this._processSet(msg, socket);
                break;

            case (CommandProcessor.SETEX):
                this._processSetEx(msg, socket);
                break;

            case (CommandProcessor.SETNX):
                this._processSetNx(msg, socket);
                break;

            case (CommandProcessor.RPUSH):
                this._processRPush(msg, socket);
                break;

            case (CommandProcessor.LPUSH):
                this._processLPush(msg, socket);
                break;

            case (CommandProcessor.GET):
                this._processGet(msg, socket);
                break;

            case (CommandProcessor.RPOP):
                this._processRPop(msg, socket);
                break;

            case (CommandProcessor.LPOP):
                this._processLPop(msg, socket);
                break;

            case (CommandProcessor.BRPOP):
                this._processBRPop(msg, socket);
                break;

            case (CommandProcessor.BLPOP):
                this._processBLPop(msg, socket);
                break;

            case (CommandProcessor.SELECT):
                this._processSelect(msg, socket);
                break;

            case CommandProcessor.DEL:
                this._processDel(msg, socket);
                break;

            default:
                this._processUnknownCommand(socket);
                break;
        }
    }

    _getCommandType(msg)
    {
        let commandType = null;

        // INFO
        if(msg.type === '*' && msg.length >= 1
            && msg.value[0].type === '$'
            && msg.value[0].value.toUpperCase() === CommandProcessor.INFO)
        {
            commandType = CommandProcessor.INFO;
        }
        else if(msg.type === '*' && msg.length >= 0
            && msg.value[0].type === '$'
            && msg.value[0].value.toUpperCase() === CommandProcessor.QUIT)
        {
            commandType = CommandProcessor.QUIT;
        }
        else if(msg.type === '*' && msg.length === 2
            && msg.value[0].type === '$'
            && msg.value[0].value.toUpperCase() === CommandProcessor.GET)
        {
            commandType = CommandProcessor.GET;
        }
        else if(msg.type === '*' && msg.length >= 3
            && msg.value[0].type === '$'
            && msg.value[0].value.toUpperCase() === CommandProcessor.SET)
        {
            commandType = CommandProcessor.SET;
        }
        else if(msg.type === '*' && msg.length >= 4
            && msg.value[0].type === '$'
            && msg.value[0].value.toUpperCase() === CommandProcessor.SETEX)
        {
            commandType = CommandProcessor.SETEX;
        }
        else if(msg.type === '*' && msg.length >= 4
            && msg.value[0].type === '$'
            && msg.value[0].value.toUpperCase() === CommandProcessor.SETNX)
        {
            commandType = CommandProcessor.SETEX;
        }
        else if(msg.type === '*' && msg.length === 3
            && msg.value[0].type === '$'
            && msg.value[0].value.toUpperCase() === CommandProcessor.LPUSH)
        {
            commandType = CommandProcessor.LPUSH;
        }
        else if(msg.type === '*' && msg.length === 3
            && msg.value[0].type === '$'
            && msg.value[0].value.toUpperCase() === CommandProcessor.RPUSH)
        {
            commandType = CommandProcessor.RPUSH;
        }
        else if(msg.type === '*' && msg.length === 2
            && msg.value[0].type === '$'
            && msg.value[0].value.toUpperCase() === CommandProcessor.LPOP)
        {
            commandType = CommandProcessor.LPOP;
        }
        else if(msg.type === '*' && msg.length === 2
            && msg.value[0].type === '$'
            && msg.value[0].value.toUpperCase() === CommandProcessor.RPOP)
        {
            commandType = CommandProcessor.RPOP;
        }
        else if(msg.type === '*' && msg.length >= 2
            && msg.value[0].type === '$'
            && msg.value[0].value.toUpperCase() === CommandProcessor.BLPOP)
        {
            commandType = CommandProcessor.BLPOP;
        }
        else if(msg.type === '*' && msg.length >= 2
            && msg.value[0].type === '$'
            && msg.value[0].value.toUpperCase() === CommandProcessor.BRPOP)
        {
            commandType = CommandProcessor.BRPOP;
        }
        else if(msg.type === '*' && msg.length === 2
            && msg.value[0].type === '$'
            && msg.value[0].value.toUpperCase() === CommandProcessor.SELECT)
        {
            commandType = CommandProcessor.SELECT;
        }
        else if(msg.type === '*' && msg.length >= 2
            && msg.value[0].type === '$'
            && msg.value[0].value.toUpperCase() === CommandProcessor.DEL)
        {
            commandType = CommandProcessor.DEL;
        }

        return commandType;
    }

    _processBlockedQueue(socket)
    {
        const messages = this.blockedQueue.filter((obj) => obj[1] === socket);
        messages.forEach((obj) => this._sendMessage(obj[0], obj[1]));
    }

    _sendMessage(msg, socket)
    {
        if(socket.blocking)
        {
            this.blockedQueue.push([ msg, socket ]);
        }
        else
        {
            const respString = this.messageParser.toString(msg);
            debug(`Send response of\n${ respString }\nto ${ socket.remoteAddress }:${ socket.remotePort }`);
            socket.write(respString);
        } // end if
    }

    _sendError(errMsg, socket)
    {
        const respMsg = {
            type: '-',
            value: `ERR ${ errMsg }`
        };
        this._sendMessage(respMsg, socket);
    }

    _sendNullReply(socket)
    {
        const respMsg = {
            type: '$',
            length: -1
        };
        this._sendMessage(respMsg, socket);
    }

    _processUnknownCommand(socket)
    {
        this._sendError('unknown command', socket);
    }

    _processInfo(msg, socket)
    {
        let infoString = '';
        infoString += '# Server\r\n';
        infoString += 'redis_version:3.0.0\r\n';
        infoString += '# Clients\r\n';
        infoString += '# Memory\r\n';
        infoString += '# Persistence\r\n';
        infoString += '# Stats\r\n';
        infoString += '# Replication\r\n';
        infoString += '# CPU\r\n';
        infoString += '# Cluster\r\n';
        infoString += '# Keyspace\r\n';
        infoString += 'db0:keys=1997,expires=1,avg_ttl=98633637897';

        // Generate our info message
        const respMsg = {
            type: '$',
            length: infoString.length,
            value: infoString
        };
        this._sendMessage(respMsg, socket);
    }

    _processQuit(msg, socket)
    {
        // 'Close connection' with OK code.
        const respMsg = {
            type: '+',
            value: 'OK'
        };
        this._sendMessage(respMsg, socket);
        socket.destroy();
    }

    _processSelect(msg, socket)
    {
        const db = msg.value[1].value;
        debug(`Selecting database: ${ db }`);
        socket.database = db;
        this.database.createDatabase(db);
        const respMsg = {
            type: '+',
            value: 'OK'
        };
        this._sendMessage(respMsg, socket);
    }

    _processGet(msg, socket)
    {
        const key = msg.value[1].value;
        debug(`Get ${ key }`);

        const value = this.database.get(key, socket.database);
        if(value)
        {
            const respMsg = {
                type: '$',
                length: value.value.length,
                value: value.value
            };
            this._sendMessage(respMsg, socket);
        }
        else
        {
            this._sendNullReply(socket);
        }
    }

    _processRPush(msg, socket)
    {
        const key = msg.value[1].value;
        const val = msg.value[2].value;
        debug(`RPUSH ${ key } ${ val }`);

        let value = this.database.get(key, socket.database);
        if(value)
        {
            if(Array.isArray(value.value))
            {
                value.value.push(val);
            }
            else
            {
                // TODO: Technically, we should error if the key is not a list.
                this._sendNullReply(socket);
            }
        }
        else
        {
            value = { value: [ val ] };
        }

        // Set the value back in the db
        this.database.set(key, value.value, socket.database);

        // Emit, for blocking support
        this.emit(`push:${ key }`, value.value);

        const arrLen = `${ value.value.length }`;
        const respMsg = {
            type: '$',
            length: arrLen.length,
            value: arrLen
        };
        this._sendMessage(respMsg, socket);
    }

    _processLPush(msg, socket)
    {
        const key = msg.value[1].value;
        const val = msg.value[2].value;
        debug(`LPUSH ${ key } ${ val }`);

        let value = this.database.get(key, socket.database);
        if(value)
        {
            if(Array.isArray(value.value))
            {
                value.value.unshift(val);
            }
            else
            {
                // TODO: Technically, we should error if the key is not a list.
                this._sendNullReply(socket);
            }
        }
        else
        {
            value = { value: [ val ] };
        }

        // Set the value back in the db
        this.database.set(key, value.value, socket.database);

        // Emit, for blocking support
        this.emit(`push:${ key }`, value.value);

        const arrLen = `${ value.value.length }`;
        const respMsg = {
            type: '$',
            length: arrLen.length,
            value: arrLen
        };
        this._sendMessage(respMsg, socket);
    }

    _processRPop(msg, socket, includeKey)
    {
        const key = msg.value[1].value;
        debug(`RPOP ${ key }`);

        const value = this.database.get(key, socket.database);
        if(value && Array.isArray(value.value) && value.value.length > 0)
        {
            const val = `${ value.value.pop() }`;
            let respMsg = {
                type: '$',
                length: val.length,
                value: val
            };

            if(includeKey)
            {
                respMsg = {
                    type: '*',
                    length: 2,
                    value: [
                        {
                            type: '$',
                            length: key.length,
                            value: key
                        },
                        respMsg
                    ]
                };
            }

            this._sendMessage(respMsg, socket);
        }
        else
        {
            // TODO: Technically, we should error if the key is not a list.
            this._sendNullReply(socket);
        }
    }

    _processLPop(msg, socket, includeKey)
    {
        const key = msg.value[1].value;
        debug(`LPOP ${ key }`);

        const value = this.database.get(key, socket.database);
        if(value && Array.isArray(value.value) && value.value.length > 0)
        {
            const val = `${ value.value.shift() }`;
            let respMsg = {
                type: '$',
                length: val.length,
                value: val
            };

            if(includeKey)
            {
                respMsg = {
                    type: '*',
                    length: 2,
                    value: [
                        {
                            type: '$',
                            length: key.length,
                            value: key
                        },
                        respMsg
                    ]
                };
            }

            this._sendMessage(respMsg, socket);
        }
        else
        {
            // TODO: Technically, we should error if the key is not a list.
            this._sendNullReply(socket);
        }
    }

    _blockOnKey(key, timeout, socket, popFunc, cleanup)
    {
        let handle;
        if(typeof timeout === 'number' && timeout > 0)
        {
            // Handle the timeout property
            handle = setTimeout(() =>
            {
                cleanup();
                socket.blocking = false;
                // eslint-disable-next-line no-use-before-define
                this.removeListener(`push:${ key }`, delayedPop);
                this._sendNullReply(socket);
                this._processBlockedQueue(socket);
            }, timeout * 1000);
        }

        // Once the event fires, we can process everything.
        const delayedPop = () =>
        {
            if(typeof timeout === 'number' && timeout > 0)
            {
                clearTimeout(handle);
            }

            // Stop blocking
            socket.blocking = false;

            // Do pop, and clean up
            popFunc();
            cleanup();

            // Process any other messages we queue up.
            this._processBlockedQueue(socket);
        };

        // Listen once per call
        this.once(`push:${ key }`, delayedPop);

        // Return the function so that we can collect them for cleanup layer.
        return delayedPop;
    }

    _processBRPop(msg, socket)
    {
        if(msg.value.length === 2)
        {
            msg.value.push({ type: '$', length: 1, value: '0' })
        }

        // The last element is always the timeout
        let timeout = parseInt(msg.value.pop().value);
        const keys = msg.value.slice(1).map((val) => val.value);
        const delayed = [];

        if(isNaN(timeout))
        {
            timeout = 0;
        }

        if(keys.length === 0 || isNaN(timeout))
        {
            this._sendError('wrong number of arguments for \'brpop\' command', socket);
        }
        else
        {
            for(const key of keys)
            {
                const innerMsg = {
                    type: '*',
                    length: 3,
                    value: [
                        { type: '$', length: 5, value: 'rpop' },
                        { type: '$', length: 3, value: key }
                    ]
                };

                const value = this.database.get(key, socket.database);
                if(value && Array.isArray(value.value) && value.value.length > 0)
                {
                    // The easy case is we've got a key waiting. We just treat it like a pop.
                    this._processRPop(innerMsg, socket, true);
                    break;
                }
                else
                {
                    // In this case, we've gotta wait, and do some cleanup because once any of them resolves, we cancel the
                    // others. Turns out, that's a little complicated. Also, there's a timeout to handle.
                    socket.blocking = true;
                    const delayFunc = this._blockOnKey(
                        key, timeout, socket,
                        () =>
                        {
                            this._processRPop(innerMsg, socket, true);
                        },
                        () =>
                        {
                            delayed.forEach(({ key, delayFunc }) => this.removeAllListeners(`push:${ key }`, delayFunc));
                        }
                    );

                    // Store for cleanup; since once any key pops, we cancel all other requests
                    delayed.push({ key, delayFunc });
                }
            }
        }
    }

    _processBLPop(msg, socket)
    {
        if(msg.value.length === 2)
        {
            msg.value.push({ type: '$', length: 1, value: '0' })
        }

        // The last element is always the timeout
        let timeout = parseInt(msg.value.pop().value);
        const keys = msg.value.slice(1).map((val) => val.value);
        const delayed = [];

        if(isNaN(timeout))
        {
            timeout = 0;
        }

        if(keys.length === 0)
        {
            this._sendError('wrong number of arguments for \'blpop\' command', socket);
        }
        else
        {
            for(const key of keys)
            {
                const innerMsg = {
                    type: '*',
                    length: 3,
                    value: [
                        { type: '$', length: 5, value: 'lpop' },
                        { type: '$', length: 3, value: key }
                    ]
                };

                const value = this.database.get(key, socket.database);
                if(value && Array.isArray(value.value) && value.value.length > 0)
                {
                    // The easy case is we've got a key waiting. We just treat it like a pop.
                    this._processLPop(innerMsg, socket, true);
                    break;
                }
                else
                {
                    // In this case, we've gotta wait, and do some cleanup because once any of them resolves, we cancel the
                    // others. Turns out, that's a little complicated. Also, there's a timeout to handle.
                    socket.blocking = true;
                    const delayFunc = this._blockOnKey(
                        key, timeout, socket,
                        () =>
                        {
                            this._processLPop(innerMsg, socket, true);
                        },
                        () =>
                        {
                            delayed.forEach(({ key, delayFunc }) => this.removeAllListeners(`push:${ key }`, delayFunc));
                        }
                    );

                    // Store for cleanup; since once any key pops, we cancel all other requests
                    delayed.push({ key, delayFunc });
                }
            }
        }
    }

    _processSet(msg, socket)
    {
        const respMsg = {
            type: '+',
            value: 'OK'
        };
        const key = msg.value[1].value;
        const value = msg.value[2].value;
        debug(`Set ${ key } to %j`, value);
        const options = {
            expiresIn: -1,
            notExists: false,
            exists: false
        };

        // Go through options and set them
        for(let i = 3; i < msg.value.length; i++)
        {
            debug('Process SET option of %j', msg.value[i]);
            if(msg.value[i].value === 'NX')
            {
                debug('Setting SET option notExists to true');
                options.notExists = true;
            }
            else if(msg.value[i].value === 'XX')
            {
                debug('Setting SET option exists to true');
                options.exists = true;
            }
            else if(msg.value[i].value === 'EX')
            {
                const expiresIn = parseInt(msg.value[i + 1].value);
                debug(`Setting SET option expiresIn to ${ expiresIn }`);
                i += 1;
                options.expiresIn = expiresIn;
            }
            else if(msg.value[i].value === 'PX')
            {
                const expiresIn = parseInt(msg.value[i + 1].value) / 1000;
                debug(`Setting SET option expiresIn to ${ expiresIn }`);
                i += 1;
                options.expiresIn = expiresIn;
            }
            else
            {
                debug('Skipping unknown option');
            }
        }
        if(this.database.set(key, value, socket.database, options))
        {
            this._sendMessage(respMsg, socket);
        }
        else
        {
            error('Failed to set');
            this._sendNullReply(socket);
        }
    }

    _processSetEx(msg, socket)
    {
        debug('Transform SETEX into SET command');
        const command = {
            type: '*',
            length: 4,
            value: [
                {
                    type: '$',
                    value: 'set',
                    length: 3
                },
                msg.value[1],
                msg.value[2],
                {
                    type: '$',
                    value: 'EX',
                    length: 2
                },
                msg.value[3]
            ]
        };
        this._processSet(command, socket);
    }

    _processSetNx(msg, socket)
    {
        debug('Transform SETNX into SET command');
        const command = {
            type: '*',
            length: 4,
            value: [
                {
                    type: '$',
                    value: 'set',
                    length: 3
                },
                msg.value[1],
                msg.value[2],
                {
                    type: '$',
                    value: 'NX',
                    length: 2
                }
            ]
        };
        this._processSet(command, socket);
    }

    _processDel(msg, socket)
    {
        let deleted = 0;
        // Go through all keys and delete them
        for(let i = 1; i < msg.value.length; i++)
        {
            const key = msg.value[i].value;
            debug('Del', key);
            if(this.database.del(key, socket.database))
            {
                deleted++;
            }
        }
        const respMsg = {
            type: ':',
            value: deleted
        };
        this._sendMessage(respMsg, socket);
    }

    static get INFO()
    {
        return 'INFO';
    }

    static get QUIT()
    {
        return 'QUIT';
    }

    static get SELECT()
    {
        return 'SELECT';
    }

    static get SET()
    {
        return 'SET';
    }

    static get SETEX()
    {
        return 'SETEX';
    }


    static get SETNX()
    {
        return 'SETNX';
    }

    static get LPUSH()
    {
        return 'LPUSH';
    }

    static get RPUSH()
    {
        return 'RPUSH';
    }

    static get GET()
    {
        return 'GET';
    }

    static get LPOP()
    {
        return 'LPOP';
    }

    static get RPOP()
    {
        return 'RPOP';
    }

    static get BLPOP()
    {
        return 'BLPOP';
    }

    static get BRPOP()
    {
        return 'BRPOP';
    }

    static get DEL()
    {
        return 'DEL';
    }
};
