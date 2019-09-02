'use strict';

const MessageParser = require('../../src/message-parser.js');

require('../common');

describe('MessageParser', () =>
{

    let messageParser;

    beforeEach(() =>
    {
        messageParser = new MessageParser();
    });

    describe('#getCommands()', () =>
    {

        it('should return commands', () =>
        {
            let data = '*1\r\n$4\r\ninfo\r\n*2\r\n$6\r\nselect\r\n$1\r\n1\r\n';
            let commands = messageParser.getCommands(data);
            should.exist(commands);
            commands.length.should.equal(2);
        });

    });

    describe('#parse()', () =>
    {

        it('should fail due to null input', () =>
        {
            should.not.exist(messageParser.parse());
            should.not.exist(messageParser.parse(null));
            should.not.exist(messageParser.parse(undefined));
        });

        it('should fail due to invalid input', () =>
        {
            should.not.exist(messageParser.parse('bad'));
            should.not.exist(messageParser.parse(123));
            should.not.exist(messageParser.parse({}));
            should.not.exist(messageParser.parse(true));
        });

        it('should fail due to invalid length', () =>
        {
            should.not.exist(messageParser.parse(Buffer.from([ 0x00, 0x00 ])));
        });

        it('should fail due to invalid end of message', () =>
        {
            should.not.exist(messageParser.parse(Buffer.from('000')));
            should.not.exist(messageParser.parse(Buffer.from('0\r0')));
            should.not.exist(messageParser.parse(Buffer.from('00\n')));
        });

        it('should fail due to invalid type', () =>
        {
            let data = Buffer.from('#\r\n');
            should.not.exist(messageParser.parse(data));
        });

        it('should parse error', () =>
        {
            let data = Buffer.from('-ERR\r\n');
            let msg = messageParser.parse(data);
            should.exist(msg);
            msg.type.should.equal('-');
            msg.value.should.equal('ERR');
        });

        it('should parse simple string', () =>
        {
            let data = Buffer.from('+OK\r\n');
            let msg = messageParser.parse(data);
            should.exist(msg);
            msg.type.should.equal('+');
            msg.value.should.equal('OK');
        });

        it('should fail to parse bulk string due to invalid length', () =>
        {
            let data = Buffer.from('$20\r\nbulkstring\r\n');
            let msg = messageParser.parse(data);
            should.not.exist(msg);
        });

        it('should parse bulk string', () =>
        {
            let data = Buffer.from('$10\r\nbulkstring\r\n');
            let msg = messageParser.parse(data);
            should.exist(msg);
            msg.type.should.equal('$');
            msg.length.should.equal(10);
            msg.value.should.equal('bulkstring');
        });

        it('should parse null bulk string', () =>
        {
            let data = Buffer.from('$-1\r\n');
            let msg = messageParser.parse(data);
            should.exist(msg);
            msg.type.should.equal('$');
            msg.length.should.equal(-1);
            should.not.exist(msg.value);
        });

        it('should fail to parse array due to uknown type', () =>
        {
            let data = Buffer.from('*3\r\n$8\r\nsentinel\r\n$23\r\nget-master-addr-by-name\r\n^8\r\nmymaster\r\n');
            let msg = messageParser.parse(data);
            should.not.exist(msg);
        });

        it('should parse array', () =>
        {
            let data = Buffer.from('*3\r\n$8\r\nsentinel\r\n$23\r\nget-master-addr-by-name\r\n$8\r\nmymaster\r\n');
            let msg = messageParser.parse(data);
            should.exist(msg);
            msg.type.should.equal('*');
            msg.length.should.equal(3);
            msg.value.length.should.equal(3);
            msg.value[ 0 ].type.should.equal('$');
            msg.value[ 0 ].length.should.equal(8);
            msg.value[ 0 ].value.should.equal('sentinel');
            msg.value[ 1 ].type.should.equal('$');
            msg.value[ 1 ].length.should.equal(23);
            msg.value[ 1 ].value.should.equal('get-master-addr-by-name');
            msg.value[ 2 ].type.should.equal('$');
            msg.value[ 2 ].length.should.equal(8);
            msg.value[ 2 ].value.should.equal('mymaster');
        });

    });

    describe('#toString()', () =>
    {

        it('should fail due to unknown type', () =>
        {
            let msg = {
                type: 'bad'
            };
            let str = messageParser.toString(msg);
            should.not.exist(str);
        });

        it('should convert error to string', () =>
        {
            let msg = {
                type: '-',
                value: 'ERR'
            };
            let str = messageParser.toString(msg);
            should.exist(str);
            str.should.equal('-ERR\r\n');
        });

        it('should convert simple string to string', () =>
        {
            let msg = {
                type: '+',
                value: 'OK'
            };
            let str = messageParser.toString(msg);
            should.exist(str);
            str.should.equal('+OK\r\n');
        });

        it('should convert bulk string to string', () =>
        {
            let msg = {
                type: '$',
                length: 10,
                value: 'bulkstring'
            };
            let str = messageParser.toString(msg);
            should.exist(str);
            str.should.equal('$10\r\nbulkstring\r\n');
        });

        it('should convert null string to string', () =>
        {
            let msg = {
                type: '$',
                length: -1
            };
            let str = messageParser.toString(msg);
            should.exist(str);
            str.should.equal('$-1\r\n');
        });

        it('should convert array to string', () =>
        {
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
                    }
                ]
            };
            let str = messageParser.toString(msg);
            should.exist(str);
            str.should.equal('*3\r\n$8\r\nsentinel\r\n$23\r\nget-master-addr-by-name\r\n$8\r\nmymaster\r\n');
        });

    });

});
