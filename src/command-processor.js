'use strict';

const MessageParser = require('./message-parser');
const Database = require('./database');

const debug = require('debug')('redmock:command-processor');
const error = require('debug')('redmock:error');

module.exports = class CommandProcessor
{
    constructor()
    {
        error.color = 1;
        this.messageParser = new MessageParser();
        this.database = new Database();
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

            case (CommandProcessor.GET):
                this._processGet(msg, socket);
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

    _sendMessage(msg, socket)
    {
        const respString = this.messageParser.toString(msg);
        debug(`Send response of\n${ respString }\nto ${ socket.remoteAddress }:${ socket.remotePort }`);
        socket.write(respString);
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

    static get GET()
    {
        return 'GET';
    }

    static get DEL()
    {
        return 'DEL';
    }
};
