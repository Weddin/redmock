# RedMock

Mock [Redis](http://redis.io) server for [Node](https://nodejs.org) unit tests.

Requires Node version 6.0.0 or higher for the newest language features.

## Purpose

This project was created to help unit test applications using Redis. Stubbing out the redis client to perform unit tests is problematic, because it requires tying tests to the api of a dependency, not the application code. Version upgrades, or other dependency changes. Instead, RedMock can be used to do full integration testing, without needing to start a real redis server.

## Fork

This project has been forked to update dependencies and try to bring the code base into a more modern state, thanks to advances in node.js since v5. The code is being cleaned up, and examples/tests are being updated.

## Usage

### Starting the server

Call the start method after creating a new instance of the RedisServer class. This method returns an ES6 promise.

```javascript
const RedisServer = require('@skewedaspect/redmock');

const redisServer = new RedisServer();

redisServer
    .start()
    .then((res) =>
    {
        // Server is now up
    })
    .catch((err) =>
    {
        // Deal with error
    });
```

### Stopping the server

Call the stop method. This method returns a promise. (This message does not error.)

```javascript
redisServer
    .stop()
    .then((res) =>
    {
        // Server is now stopped
    });
```

### Example test

```javascript
// require needed modules

describe('SomeTestSpec', () =>
{
    let redisServer, underTest;

    // Start the server
    before(() =>
    {
        redisServer = new RedisServer();
        return redisServer.start();
    });

    // Stop the server
    after(() =>
    {
        return redisServer.stop();
    });

    describe('#somemethod()', () =>
    {
        let underTest;
        beforeEach(() => 
        {
            underTest = new UnderTest();
        });

        it('should test it', () =>
        {
            return underTest.somemethod().should.eventually.equal(true);
        });
    });
});
```
