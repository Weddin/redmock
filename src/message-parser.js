'use strict';

/**
 * Message parser.
 */
module.exports = class MessageParser {

  /**
   * Constructor.
   * @constructor
   */
  constructor() {
  }

  /**
   * Takes a message and converts into a string that can be sent
   * over the wire.
   */
  toString(msg) {
    let res = null;
    switch(msg.type) {
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

  /**
   * Get commands from a buffer.
   */
  getCommands(data) {
    let commands = data.toString().match(/[^*]+/g);
    let res = [ ];
    for (let command of commands) {
      res.push(new Buffer('*' + command));
    }
    return res;
  }

  /**
   * Parse a buffer.
   * @param {Buffer} data - The buffer.
   */
  parse(data) {
    if (!data) {
      return null;
    }

    // Must be a buffer
    if (!Buffer.isBuffer(data)) {
      return null;
    }

    // No point trying if length is less than 3
    if (data.length < 3) {
      return null;
    }

    // Make sure message ends with \r\n
    if (data[data.length - 1] != 10 || data[data.length - 2] != 13) {
      return null;
    }

    // Determine message type
    let res = this._determineTypeAndParse(data);

    return res;
  }

  /**
   * Convert an error into a string.
   */
  _errorToString(msg) {
    let res = '';
    res += '-' + msg.value + '\r\n';
    return res;
  }

  /**
   * Convert a simple string into a string.
   */
  _simpleStringToString(msg) {
    let res = '';
    res += '+' + msg.value + '\r\n';
    return res;
  }

  /**
   * Convert a bulk string into a string.
   */
  _bulkStringToString(msg) {
    let res = '';
    // Length
    res += '$' + msg.length;
    if (msg.length <= 0) {
      res += '\r\n';
    } else {
      res += '\r\n' + msg.value + '\r\n';
    }
    return res;
  }

  /**
   * Convert an array message into a string.
   */
  _arrayToString(msg) {
    let res = '';
    // Length
    res += '*' + msg.length + '\r\n';
    for (let val of msg.value) {
      res += this.toString(val);
    }
    return res;
  }

  /**
   * Convert an integer message into a string.
   */
  _integerToString(msg) {
    let res = '';
    res += ':' + Math.ceil(msg.value).toString() + '\r\n';
    return res;
  }

  /**
   * Determine message type.
   */
  _determineTypeAndParse(data) {
    let res = null;
    let c = String.fromCharCode(data[0]);
    switch (c) {
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

  /**
   * Parse an error.
   */
  _parseError(data) {
    let string = data.slice(1, data.length - 2).toString();
    return {
      type: MessageParser.ERROR,
      value: string
    };
  }

  /**
   * Parse a simple string.
   */
  _parseSimpleString(data) {
    let string = data.slice(1, data.length - 2).toString();
    return {
      type: MessageParser.SIMPLE_STRING,
      value: string
    };
  }

  /**
   * Parse a bulk string.
   */
  _parseBulkString(data) {
    let lines = data.toString().split('\r\n');
    // Line 1 is the length of the string
    let len = parseInt(lines[0].substring(1));
    let value = null;
    if (len == -1) {
      value = null;
    } else if (len != lines[1].length) {
      return null;
    } else {
      value = lines[1];
    }
    return {
      type: MessageParser.BULK_STRING,
      value: value,
      length: len
    };
  }

  /**
   * Parse an array. Damn ugly.
   */
  _parseArray(data) {
    let elementsProcessed = 0;
    let unknownType = false;
    let dataString = data.toString();
    let firstLine = dataString.split('\r\n', 1);
    let numElements = parseInt(firstLine[0].substring(1));

    let startOfElements = data.toString().indexOf('\r\n') + 2;
    dataString = dataString.substring(startOfElements);

    let res = {
      type: MessageParser.ARRAY,
      length: numElements,
      value: [
      ]
    };

    while (elementsProcessed < numElements && unknownType === false) {
      switch(dataString[0]) {
        case MessageParser.BULK_STRING: {
          let idx = this._getPosition(dataString, '\r\n', 2);
          let msg = this._parseBulkString(dataString.substring(0, idx + 2));
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

  _getPosition(str, m, i) {
    return str.split(m, i).join(m).length;
  }

  static get ERROR() {
    return '-';
  }

  static get SIMPLE_STRING() {
    return '+';
  }

  static get BULK_STRING() {
    return '$';
  }

  static get ARRAY() {
    return '*';
  }

  static get INTEGER() {
    return ':';
  }
}
