const Redis = require('ioredis');
const config = require('../config');

class InMemoryRedisClient {
  constructor() {
    this.lists = new Map();
    this.zsets = new Map();
    this.sets = new Map();
    this.kv = new Map();
    this.expiries = new Map();
    this.waitingPops = [];
    this.waitingMoves = [];
    this.isFallback = true;
  }

  async ping() {
    return 'PONG';
  }

  _isExpired(key) {
    if (!this.expiries.has(key)) return false;
    const expiresAt = this.expiries.get(key);
    if (Date.now() > expiresAt) {
      this.kv.delete(key);
      this.lists.delete(key);
      this.zsets.delete(key);
      this.sets.delete(key);
      this.expiries.delete(key);
      return true;
    }
    return false;
  }

  async set(key, value, ...options) {
    let ttlMs = null;
    let onlyIfNotExists = false;
    let onlyIfExists = false;

    for (let i = 0; i < options.length; i++) {
      const opt = String(options[i]).toUpperCase();
      if (opt === 'EX' && options[i + 1] !== undefined) {
        ttlMs = parseInt(options[i + 1], 10) * 1000;
        i++;
      } else if (opt === 'PX' && options[i + 1] !== undefined) {
        ttlMs = parseInt(options[i + 1], 10);
        i++;
      } else if (opt === 'NX') {
        onlyIfNotExists = true;
      } else if (opt === 'XX') {
        onlyIfExists = true;
      }
    }

    const keyExists = this.kv.has(key) && !this._isExpired(key);

    if (onlyIfNotExists && keyExists) {
      return null;
    }
    if (onlyIfExists && !keyExists) {
      return null;
    }

    this.kv.set(key, String(value));
    if (ttlMs !== null) {
      this.expiries.set(key, Date.now() + ttlMs);
    } else {
      this.expiries.delete(key);
    }

    return 'OK';
  }

  async get(key) {
    if (this._isExpired(key)) return null;
    return this.kv.has(key) ? this.kv.get(key) : null;
  }

  async del(...keys) {
    let deleted = 0;
    for (const key of keys) {
      this._isExpired(key);
      let found = false;
      if (this.kv.delete(key)) found = true;
      if (this.lists.delete(key)) found = true;
      if (this.zsets.delete(key)) found = true;
      if (this.sets.delete(key)) found = true;
      this.expiries.delete(key);
      if (found) deleted++;
    }
    return deleted;
  }

  async exists(...keys) {
    let count = 0;
    for (const key of keys) {
      if (!this._isExpired(key)) {
        if (this.kv.has(key) || this.lists.has(key) || this.zsets.has(key) || this.sets.has(key)) {
          count++;
        }
      }
    }
    return count;
  }

  async lpush(key, ...values) {
    this._isExpired(key);
    if (!this.lists.has(key)) this.lists.set(key, []);
    const list = this.lists.get(key);
    for (const val of values) {
      list.unshift(String(val));
      this._checkWaitingPops(key);
      this._checkWaitingMoves(key);
    }
    return list.length;
  }

  async rpush(key, ...values) {
    this._isExpired(key);
    if (!this.lists.has(key)) this.lists.set(key, []);
    const list = this.lists.get(key);
    for (const val of values) {
      list.push(String(val));
      this._checkWaitingPops(key);
      this._checkWaitingMoves(key);
    }
    return list.length;
  }

  async rpop(key) {
    this._isExpired(key);
    const list = this.lists.get(key);
    if (!list || list.length === 0) return null;
    return list.pop();
  }

  async lpop(key) {
    this._isExpired(key);
    const list = this.lists.get(key);
    if (!list || list.length === 0) return null;
    return list.shift();
  }

  async llen(key) {
    this._isExpired(key);
    const list = this.lists.get(key);
    return list ? list.length : 0;
  }

  async lrem(key, count, element) {
    this._isExpired(key);
    const list = this.lists.get(key);
    if (!list || list.length === 0) return 0;

    const elStr = String(element);
    let removed = 0;

    if (count === 0) {
      const initialLen = list.length;
      const filtered = list.filter((item) => item !== elStr);
      removed = initialLen - filtered.length;
      this.lists.set(key, filtered);
    } else if (count > 0) {
      for (let i = 0; i < list.length && removed < count; i++) {
        if (list[i] === elStr) {
          list.splice(i, 1);
          removed++;
          i--;
        }
      }
    } else {
      // count < 0, remove from right to left
      const limit = Math.abs(count);
      for (let i = list.length - 1; i >= 0 && removed < limit; i--) {
        if (list[i] === elStr) {
          list.splice(i, 1);
          removed++;
        }
      }
    }

    return removed;
  }

  async lrange(key, start, stop) {
    this._isExpired(key);
    const list = this.lists.get(key);
    if (!list) return [];
    const len = list.length;
    let s = start < 0 ? Math.max(len + start, 0) : start;
    let e = stop < 0 ? len + stop : stop;
    if (s > e || s >= len) return [];
    return list.slice(s, e + 1);
  }

  /**
   * Atomic LMOVE / RPOPLPUSH implementation
   */
  async lmove(source, destination, wherefrom = 'RIGHT', whereto = 'LEFT') {
    this._isExpired(source);
    this._isExpired(destination);

    const srcList = this.lists.get(source);
    if (!srcList || srcList.length === 0) return null;

    const item = String(wherefrom).toUpperCase() === 'RIGHT' ? srcList.pop() : srcList.shift();

    if (!this.lists.has(destination)) this.lists.set(destination, []);
    const destList = this.lists.get(destination);

    if (String(whereto).toUpperCase() === 'LEFT') {
      destList.unshift(item);
    } else {
      destList.push(item);
    }

    this._checkWaitingPops(destination);
    this._checkWaitingMoves(destination);
    return item;
  }

  async rpoplpush(source, destination) {
    return this.lmove(source, destination, 'RIGHT', 'LEFT');
  }

  /**
   * Atomic BLMOVE implementation with blocking wait
   */
  async blmove(source, destination, wherefrom = 'RIGHT', whereto = 'LEFT', timeoutSec = 0) {
    const immediate = await this.lmove(source, destination, wherefrom, whereto);
    if (immediate !== null) return immediate;

    if (timeoutSec === 0) {
      return new Promise((resolve) => {
        this.waitingMoves.push({ source, destination, wherefrom, whereto, resolve });
      });
    }

    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        const idx = this.waitingMoves.findIndex((w) => w.timer === timer);
        if (idx !== -1) this.waitingMoves.splice(idx, 1);
        resolve(null);
      }, timeoutSec * 1000);

      this.waitingMoves.push({ source, destination, wherefrom, whereto, resolve, timer });
    });
  }

  async brpoplpush(source, destination, timeoutSec = 0) {
    return this.blmove(source, destination, 'RIGHT', 'LEFT', timeoutSec);
  }

  _checkWaitingMoves(sourceKey) {
    for (let i = 0; i < this.waitingMoves.length; i++) {
      const waiter = this.waitingMoves[i];
      if (waiter.source === sourceKey) {
        const srcList = this.lists.get(sourceKey);
        if (srcList && srcList.length > 0) {
          if (waiter.timer) clearTimeout(waiter.timer);
          this.waitingMoves.splice(i, 1);

          const item = String(waiter.wherefrom).toUpperCase() === 'RIGHT' ? srcList.pop() : srcList.shift();
          if (!this.lists.has(waiter.destination)) this.lists.set(waiter.destination, []);
          const destList = this.lists.get(waiter.destination);

          if (String(waiter.whereto).toUpperCase() === 'LEFT') {
            destList.unshift(item);
          } else {
            destList.push(item);
          }

          waiter.resolve(item);
          return;
        }
      }
    }
  }

  async brpop(...args) {
    const timeout = typeof args[args.length - 1] === 'number' ? args.pop() : 0;
    const keys = args;

    for (const key of keys) {
      this._isExpired(key);
      const list = this.lists.get(key);
      if (list && list.length > 0) {
        return [key, list.pop()];
      }
    }

    if (timeout === 0) {
      return new Promise((resolve) => {
        this.waitingPops.push({ keys, resolve });
      });
    }

    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        const idx = this.waitingPops.findIndex((w) => w.timer === timer);
        if (idx !== -1) this.waitingPops.splice(idx, 1);
        resolve(null);
      }, timeout * 1000);

      this.waitingPops.push({ keys, resolve, timer });
    });
  }

  _checkWaitingPops(key) {
    for (let i = 0; i < this.waitingPops.length; i++) {
      const waiter = this.waitingPops[i];
      if (waiter.keys.includes(key)) {
        const list = this.lists.get(key);
        if (list && list.length > 0) {
          if (waiter.timer) clearTimeout(waiter.timer);
          this.waitingPops.splice(i, 1);
          const item = list.pop();
          waiter.resolve([key, item]);
          return;
        }
      }
    }
  }

  async zadd(key, score, member) {
    if (!this.zsets.has(key)) this.zsets.set(key, new Map());
    this.zsets.get(key).set(String(member), Number(score));
    return 1;
  }

  async zrangebyscore(key, min, max) {
    const zset = this.zsets.get(key);
    if (!zset) return [];
    const minVal = min === '-inf' ? -Infinity : parseFloat(min);
    const maxVal = max === '+inf' ? Infinity : parseFloat(max);
    const results = [];
    for (const [member, score] of zset.entries()) {
      if (score >= minVal && score <= maxVal) {
        results.push(member);
      }
    }
    return results;
  }

  async zrem(key, ...members) {
    const zset = this.zsets.get(key);
    if (!zset) return 0;
    let count = 0;
    for (const member of members) {
      if (zset.delete(String(member))) count++;
    }
    return count;
  }

  async zcard(key) {
    const zset = this.zsets.get(key);
    return zset ? zset.size : 0;
  }

  async sadd(key, ...members) {
    if (!this.sets.has(key)) this.sets.set(key, new Set());
    const set = this.sets.get(key);
    let count = 0;
    for (const m of members) {
      const str = String(m);
      if (!set.has(str)) {
        set.add(str);
        count++;
      }
    }
    return count;
  }

  async srem(key, ...members) {
    const set = this.sets.get(key);
    if (!set) return 0;
    let count = 0;
    for (const m of members) {
      if (set.delete(String(m))) count++;
    }
    return count;
  }

  async scard(key) {
    const set = this.sets.get(key);
    return set ? set.size : 0;
  }

  async smembers(key) {
    const set = this.sets.get(key);
    return set ? Array.from(set.values()) : [];
  }

  async flushall() {
    this.lists.clear();
    this.zsets.clear();
    this.sets.clear();
    this.kv.clear();
    this.expiries.clear();
    return 'OK';
  }

  async quit() {
    return 'OK';
  }

  duplicate() {
    return this;
  }
}

let redisClient = null;
const sharedInMemory = new InMemoryRedisClient();

async function createRedisClient(label = 'primary') {
  if (config.enableInMemoryFallback) {
    try {
      const client = new Redis({
        host: config.redis.host,
        port: config.redis.port,
        password: config.redis.password,
        maxRetriesPerRequest: 1,
        connectTimeout: 1500,
        retryStrategy: () => null,
      });

      await new Promise((resolve, reject) => {
        client.once('ready', () => resolve(true));
        client.once('error', (err) => reject(err));
      });

      console.log(`[Redis:${label}] Connected to real Redis at ${config.redis.host}:${config.redis.port}`);
      return client;
    } catch (err) {
      console.log(`[Redis:${label}] External Redis unavailable (${err.message}). Using high-performance In-Memory Redis fallback.`);
      return sharedInMemory;
    }
  }

  const client = new Redis({
    host: config.redis.host,
    port: config.redis.port,
    password: config.redis.password,
  });
  return client;
}

module.exports = {
  createRedisClient,
  InMemoryRedisClient,
  sharedInMemory,
};
