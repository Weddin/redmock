'use strict';

const MessageParser = require('./message-parser');

const debug = require('debug')('redmock:sentinel-command-parser');

module.exports = class SentinelCommandProcessor
{
    constructor(redisPort)
    {
        this.redisPort = redisPort;
        this.messageParser = new MessageParser();
    }

    process(msg, socket)
    {
        const commandType = this._getCommandType(msg);
        debug(`Process sentinel command type of ${ commandType } from ${ socket.remoteAddress }:${ socket.remotePort }`);
        switch (commandType)
        {
            case SentinelCommandProcessor.GET_MASTER_ADDR_BY_NAME: {
                this._processGetMasterAddrByName(msg, socket);
                break;
            }
            default: {
                this._processUnknownCommand(socket);
                break;
            }
        }
    }

    _sendMessage(msg, socket)
    {
        const respString = this.messageParser.toString(msg);
        debug(`Sentinel send response of\n${ respString }\nto ${ socket.remoteAddress }:${ socket.remotePort }`);
        socket.write(respString);
    }

    _processUnknownCommand(socket)
    {
        const respMsg = {
            type: '-',
            value: 'ERR unknown command'
        };
        socket.write(this.messageParser.toString(respMsg));
    }

    _processGetMasterAddrByName(msg, socket)
    {
        const respMsg = {
            type: '*',
            length: 2,
            value: [
                {
                    type: '$',
                    length: 9,
                    value: '127.0.0.1'
                },
                {
                    type: '$',
                    length: 4,
                    value: '6379'
                }
            ]
        };
        this._sendMessage(respMsg, socket);
    }

    _getCommandType(msg)
    {
        let commandType = null;
        // get-master-addr-by-name
        if(msg.type === '*' && msg.length === 3
        && msg.value[0].type === '$' && msg.value[0].value === 'sentinel'
        && msg.value[1].type === '$' && msg.value[1].value === SentinelCommandProcessor.GET_MASTER_ADDR_BY_NAME)
        {
            commandType = SentinelCommandProcessor.GET_MASTER_ADDR_BY_NAME;
        }

        return commandType;
    }

    static get GET_MASTER_ADDR_BY_NAME()
    {
        return 'get-master-addr-by-name';
    }
};
