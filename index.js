"use strict";

// var Etcd = require('node-etcd');
var request = require('request');
var Promise     = require('bluebird');
var uid2 = require('uid2');
var _ = require('lodash')

require('http').globalAgent.maxSockets = 1024;

var Etcdc = function(options) {

    if (!(this instanceof Etcdc)){
        return new Etcdc(options);
    }

    options = _.partialRight(_.merge, _.defaults)(options || {}, {
        hostname: '127.0.0.1',
        port: 4001
    })

    this.options = options
}

module.exports = Etcdc;

Etcdc.prototype.get = function(key, options) {
    return this._request('GET', this._makeRequestOptions('/keys/' + key, null, options))
}

Etcdc.prototype.set = function(key, value, options) {
    return this._request('PUT', this._makeRequestOptions('/keys/' + key, value, options))
}

Etcdc.prototype.del = function(key, options) {
    return this._request('DELETE', this._makeRequestOptions('/keys/' + key, null, options))
}

Etcdc.prototype.wait = function(key, options) {
    options = options || {}
    options.wait = true
    return this._request('GET', this._makeRequestOptions('/keys/' + key, null, options))
}

Etcdc.prototype.create = function(dir, value, options) {
    return this._request('POST', this._makeRequestOptions('/keys/' + dir, value, options))
}

Etcdc.prototype.lock = function(name, ttl) {
    var self = this, value = uid2(16);

    ttl = ttl || 60; // seconds

    function _wait(index){
        return self.wait('locks_' + name, { waitIndex: index }).then(function() {
            return _setLock()
        })
    }

    function _setLock(){
        return self.set('locks_' + name, value, {
            ttl: ttl,
            prevExist: false
        })
        .catch(function(err) {
            if(err.errorCode === 105){
                return _wait(err.index)
            }
            throw err
        })
    }

    return _setLock().return(value)
}

Etcdc.prototype.unlock = function(name, value) {
    return this.del('locks_' + name, {
        prevValue: value
    })
}

Etcdc.prototype.renewLock = function(name, value, ttl) {
    ttl = ttl || 60;
    return this.set('locks_' + name, value, {
        ttl: ttl,
        prevValue: value
    })
}

Etcdc.prototype.limitLock = function(name, limit) {
    var self = this, lock, waitIndex;

    function _increment() {
        return self.lock('limit_lock_' + name).then(function(_lock) {
            lock = _lock;

            return self.get('limitCounters_' + name)
            .then(function(result) {
                waitIndex = result.modifiedIndex
                return parseInt(result.node.value, 10)
            })
            .catch(function(err) {
                if(err.errorCode !== 100){
                    throw err
                }
                waitIndex = err.index
                return 0
            })
            .then(function(count) {
                if(count < limit){
                    return self.set('limitCounters_' + name, count + 1).return(true)
                }
            })
        })
        .finally(function() {
            if(lock){
                return self.unlock('limit_lock_' + name, lock)
            }
        })
        .then(function(ok) {
            if(!ok){
                return self.wait('limitCounters_' + name, { waitIndex: waitIndex }).then(function() {
                    return _increment()
                })
            }
        })
    }

    return _increment()
}

Etcdc.prototype.limitUnlock = function(name) {
    var self = this, lock;

    return self.lock('limit_lock_' + name).then(function(_lock) {
        console.log('limitUnlock', 'locked')
        lock = _lock;
        return self.get('limitCounters_' + name).then(function(result) {
            return parseInt(result.node.value, 10)
        })
        .then(function(count) {
            console.log('limitUnlock', 'count=', count)
            return self.set('limitCounters_' + name, count - 1)
        })
    })
    .finally(function() {
        console.log('limitUnlock', 'unlocking, lock=', lock)
        if(lock){
            return self.unlock('limit_lock_' + name, lock)
        }
    })
}

Etcdc.prototype._request = function(method, options) {
    var self = this;

    options.method = method

    return new Promise(function(resolve, reject) {
        request(options, function(err, res, body) {
            if(err){
                return reject(err)
            }
            if(res && res.statusCode === 307 && res.headers && res.headers.location){
                options.url = res.headers.location
                resolve(self._request(method, options))
            }
            if(body && body.errorCode){
                reject(_.merge(new Error(), body, { _headers: res.headers }))
            }
            resolve(_.merge(body, { _headers: res.headers }))
        })
    })
}

Etcdc.prototype._makeRequestOptions = function (path, value, queryString) {
    var self = this;
    var opts = {
        url: 'http://' + self.options.hostname + ':' + self.options.port + ('/v2/' + path).replace(/\/\//, '/'),
        json: true
    }

    if(value){
        opts.form = {
            value: value
        }
    }

    if(queryString){
        opts.qs = queryString
    }

    return opts
}

