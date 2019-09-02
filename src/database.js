'use strict';

const debug = require('debug')('redmock:database');
const error = require('debug')('redmock:error');

module.exports = class Database
{
    constructor()
    {
        error.color = 1;
        this.data = {
            0: {
            }
        };
    }

    createDatabase(db)
    {
        if(!this.data[db])
        {
            debug(`Create new database ${ db }`);
            this.data[db] = {
            };
        }
    }

    get(key, db)
    {
        debug(`Get ${ key } from ${ db }`);

        if(!this.data[db])
        {
            error(`Unknown database: ${ db }`);
            return null;
        }
        else
        if(this.data[db][key])
        {
            return this.getIfNotExpired(key, db);
        }
        else
        {
            error(`Unkown key ${ key } in ${ db }`);
            return null;
        }
    }

    getIfNotExpired(key, db)
    {
        const data = this.data[db][key];
        if(data.ttl > 0)
        {
            const now = Date.now();
            const then = data.created.getTime();
            const elapsed = now - then;
            const ttl = data.ttl * 1000;
            debug(`TTL in ms ${ ttl }`);
            debug(`${ elapsed } ms have elapsed since created`);
            if(elapsed > ttl)
            {
                debug('Data has expired');
                delete this.data[db][key];
                return null;
            }
            else
            {
                return data;
            }
        }
        else
        {
            return data;
        }
    }

    set(key, value, db, opts)
    {
        opts = opts || {
            expiresIn: -1,
            notExists: false,
            exists: false
        };
        debug(`Set ${ key } in ${ db } with opts %j`, opts);

        if(!this.data[db])
        {
            error(`Unknown database: ${ db }`);
            return false;
        }
        else
        {
            const data = {
                value,
                ttl: opts.expiresIn,
                created: new Date()
            };
            // Always set if notExists or exist is false
            if(!opts.notExists && !opts.exists)
            {
                debug(`Setting ${ key } in ${ db } to %j`, data);
                this.data[db][key] = data;
                return true;
            }
            else if(opts.notExists && !opts.exists)
            {
                if(!this.data[db][key])
                {
                    debug(`Setting ${ key } in ${ db } to %j`, data);
                    this.data[db][key] = data;
                    return true;
                }
                else
                {
                    error('Not exists set to true, but value already exists');
                    return false;
                }
            }
            else if(!opts.notExists && opts.exists)
            {
                if(!this.data[db][key])
                {
                    error('Exists set to true, but value does not exist');
                    return false;
                }
                else
                {
                    debug(`Setting ${ key } in ${ db } to %j`, data);
                    this.data[db][key] = data;
                    return true;
                }
            }
            else
            {
                error('Can not set both exists and not exists at the same time');
                return false;
            }
        }
    }

    del(key, db)
    {
        if(!this.data[db])
        {
            error(`Unknown database: ${ db }`);
            return null;
        }
        else
        if(this.data[db][key])
        {
            delete this.data[db][key];
            return true;
        }
        else
        {
            return false;
        }
    }
};
