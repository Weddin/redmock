'use strict';

const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');

process.env.NODE_ENV = 'test';

global.expect = chai.expect;
global.should = chai.should();
chai.use(chaiAsPromised);
global.AssertionError = chai.AssertionError;
global.Assertion = chai.Assertion;
global.assert = chai.assert;
