const Redis = require('ioredis');
const config = require('../config');

class InMemoryRedisClient {
  constructor() {
    this.lists = new Map();
    this.zsets = new Map();
    this.sets = new Map();
    this.kv = new Map();
    this.waitingPops = [];
    this.isFallback = true;
  }

  async ping() {
    return 'PONG';
  }

  async lpush(key, ...values) {
    if (!this.lists.has(key)) this.lists.set(key, []);
    const list = this.lists.get(key);
    for (const val of values) {
      list.unshift(val);
      this._checkWaitingPops(key);
    }
    return list.length;
  }

  async rpush(key, ...values) {
    if (!this.lists.has(key)) this.lists.set(key, []);
    const list = this.lists.get(key);
    for (const val of values) {
      list.push(val);
      this._checkWaitingPops(key);
    }
    return list.length;
  }

  async rpop(key) {
    const list = this.lists.get(key);
    if (!list || list.length === 0) return null;
    return list.pop();
  }

  async lpop(key) {
    const list = this.lists.get(key);
    if (!list || list.length === 0) return null;
    return list.shift();
  }

  async llen(key) {
    const list = this.lists.get(key);
    return list ? list.length : 0;
  }

  async brpop(...args) {
    const timeout = typeof args[args.length - 1] === 'number' ? args.pop() : 0;
    const keys = args;

    // Check immediate availability in priority order
    for (const key of keys) {
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
    this.zsets.get(key).set(member, score);
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
      if (zset.delete(member)) count++;
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
      if (!set.has(m)) {
        set.add(m);
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
      if (set.delete(m)) count++;
    }
    return count;
  }

  async scard(key) {
    const set = this.sets.get(key);
    return set ? set.size : 0;
  }

  async flushall() {
    this.lists.clear();
    this.zsets.clear();
    this.sets.clear();
    this.kv.clear();
    return 'OK';
  }

  async quit() {
    return 'OK';
  }

  duplicate() {
    // Return self or shared instance so workers/queues share the same memory space in fallback mode
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
        retryStrategy: () => null, // Do not hang if offline
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
