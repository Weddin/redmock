'use strict';

module.exports = class MessageParser
{
    toString(msg)
    {
        let res = null;
        switch (msg.type)
        {
            case MessageParser.ERROR: {
                res = this._errorToString(msg);
                break;
            }
            case MessageParser.SIMPLE_STRING: {
                res = this._simpleStringToString(msg);
                break;
            }
            case MessageParser.ARRAY: {
                res = this._arrayToString(msg);
                break;
            }
            case MessageParser.BULK_STRING: {
                res = this._bulkStringToString(msg);
                break;
            }
            case MessageParser.INTEGER: {
                res = this._integerToString(msg);
                break;
            }
            default: {
                break;
            }
        }
        return res;
    }

    getCommands(data)
    {
        const commands = data.toString().match(/[^*]+/g);
        const res = [ ];
        for(const command of commands)
        {
            res.push(Buffer.from(`*${ command }`));
        }
        return res;
    }

    parse(data)
    {
        if(!data)
        {
            return null;
        }

        // Must be a buffer
        if(!Buffer.isBuffer(data))
        {
            return null;
        }

        // No point trying if length is less than 3
        if(data.length < 3)
        {
            return null;
        }

        // Make sure message ends with \r\n
        if(data[data.length - 1] !== 10 || data[data.length - 2] !== 13)
        {
            return null;
        }

        // Determine message type
        const res = this._determineTypeAndParse(data);

        return res;
    }

    _errorToString(msg)
    {
        let res = '';
        res += `-${ msg.value }\r\n`;
        return res;
    }

    _simpleStringToString(msg)
    {
        let res = '';
        res += `+${ msg.value }\r\n`;
        return res;
    }

    _bulkStringToString(msg)
    {
        let res = '';
        // Length
        res += `$${ msg.length }`;
        if(msg.length <= 0)
        {
            res += '\r\n';
        }
        else
        {
            res += `\r\n${ msg.value }\r\n`;
        }
        return res;
    }

    _arrayToString(msg)
    {
        let res = '';
        // Length
        res += `*${ msg.length }\r\n`;
        for(const val of msg.value)
        {
            res += this.toString(val);
        }
        return res;
    }

    _integerToString(msg)
    {
        let res = '';
        res += `:${ Math.ceil(msg.value).toString() }\r\n`;
        return res;
    }

    _determineTypeAndParse(data)
    {
        let res = null;
        const char = String.fromCharCode(data[0]);
        switch (char)
        {
            case MessageParser.ERROR: {
                res = this._parseError(data);
                break;
            }
            case MessageParser.SIMPLE_STRING: {
                res = this._parseSimpleString(data);
                break;
            }
            case MessageParser.BULK_STRING: {
                res = this._parseBulkString(data);
                break;
            }
            case MessageParser.ARRAY: {
                res = this._parseArray(data);
                break;
            }
            default: {
                break;
            }
        }
        return res;
    }

    _parseError(data)
    {
        const string = data.slice(1, data.length - 2).toString();
        return {
            type: MessageParser.ERROR,
            value: string
        };
    }

    _parseSimpleString(data)
    {
        const string = data.slice(1, data.length - 2).toString();
        return {
            type: MessageParser.SIMPLE_STRING,
            value: string
        };
    }

    _parseBulkString(data)
    {
        const lines = data.toString().split('\r\n');
        // Line 1 is the length of the string
        const len = parseInt(lines[0].substring(1));
        let value = null;
        if(len == -1)
        {
            value = null;
        }
        else if(len != lines[1].length)
        {
            return null;
        }
        else
        {
            value = lines[1];
        }
        return {
            type: MessageParser.BULK_STRING,
            value,
            length: len
        };
    }

    _parseArray(data)
    {
        let elementsProcessed = 0;
        let unknownType = false;
        let dataString = data.toString();
        const firstLine = dataString.split('\r\n', 1);
        const numElements = parseInt(firstLine[0].substring(1));

        const startOfElements = data.toString().indexOf('\r\n') + 2;
        dataString = dataString.substring(startOfElements);

        let res = {
            type: MessageParser.ARRAY,
            length: numElements,
            value: [
            ]
        };

        while(elementsProcessed < numElements && unknownType === false)
        {
            switch (dataString[0])
            {
                case MessageParser.BULK_STRING: {
                    const idx = this._getPosition(dataString, '\r\n', 2);
                    const msg = this._parseBulkString(dataString.substring(0, idx + 2));
                    dataString = dataString.substring(idx + 2);
                    elementsProcessed += 1;
                    res.value.push(msg);
                    break;
                }
                default: {
                    unknownType = true;
                    res = null;
                    break;
                }
            }
        }

        return res;
    }

    _getPosition(str, mdx, idx)
    {
        return str.split(mdx, idx).join(mdx).length;
    }

    static get ERROR()
    {
        return '-';
    }

    static get SIMPLE_STRING()
    {
        return '+';
    }

    static get BULK_STRING()
    {
        return '$';
    }

    static get ARRAY()
    {
        return '*';
    }

    static get INTEGER()
    {
        return ':';
    }
};
