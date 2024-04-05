import Monologue from 'node-monologue';
import { connectionFn } from './connectionFsm.js';
import topologyFn from './topology.js';
import postal from 'postal';
import * as uuid from 'uuid';

import { logger } from './log.js';

const dispatch = postal.channel('rabbit.dispatch');
const responses = postal.channel('rabbit.responses');
const signal = postal.channel('rabbit.ack');

const DEFAULT = 'default';
const confLog = logger('rabbot.configuration');

const unhandledStrategies = {
  nackOnUnhandled: function (message) {
    message.nack();
  },
  rejectOnUnhandled: function (message) {
    message.reject();
  },
  customOnUnhandled: function () {}
};

const returnedStrategies = {
  customOnReturned: function () {}
};

unhandledStrategies.onUnhandled = unhandledStrategies.nackOnUnhandled;
returnedStrategies.onReturned = returnedStrategies.customOnReturned;

const serializers = {
  'application/json': {
    deserialize: (bytes, encoding) => {
      return JSON.parse(bytes.toString(encoding || 'utf8'));
    },
    serialize: (object) => {
      const json = (typeof object === 'string')
        ? object
        : JSON.stringify(object);
      return Buffer.from(json, 'utf8');
    }
  },
  'application/octet-stream': {
    deserialize: (bytes) => {
      return bytes;
    },
    serialize: (bytes) => {
      if (Buffer.isBuffer(bytes)) {
        return bytes;
      } else if (Array.isArray(bytes)) {
        return Buffer.from(bytes);
      } else {
        throw new Error('Cannot serialize unknown data type');
      }
    }
  },
  'text/plain': {
    deserialize: (bytes, encoding) => {
      return bytes.toString(encoding || 'utf8');
    },
    serialize: (string) => {
      return Buffer.from(string, 'utf8');
    }
  }
};

export class Broker {
  constructor () {
    this.connections = {};
    this.hasHandles = false;
    this.autoNack = false;
    this.serializers = serializers;
    this.configurations = {};
    this.configuring = {};
    this.log = logger;
  }

  addConnection (opts) {
    const options = Object.assign({}, {
      name: DEFAULT,
      retryLimit: 3,
      failAfter: 60
    }, opts);
    const name = options.name;
    let connection;

    const connectionPromise = new Promise((resolve, reject) => {
      if (!this.connections[name]) {
        connection = connectionFn(options);
        const topology = topologyFn(connection, options, serializers, unhandledStrategies, returnedStrategies);

        connection.on('connected', () => {
          this.emit('connected', connection);
          this.emit(connection.name + '.connection.opened', connection);
          this.setAckInterval(500);
          resolve(topology);
        });

        connection.on('closed', () => {
          this.emit('closed', connection);
          this.emit(connection.name + '.connection.closed', connection);
          reject(new Error('connection closed'));
        });

        connection.on('failed', (err) => {
          this.emit('failed', connection);
          this.emit(name + '.connection.failed', err);
          reject(err);
        });

        connection.on('unreachable', () => {
          this.emit('unreachable', connection);
          this.emit(name + '.connection.unreachable');
          this.clearAckInterval();
          reject(new Error('connection unreachable'));
        });

        connection.on('return', (raw) => {
          this.emit('return', raw);
        });
        this.connections[name] = topology;
      } else {
        connection = this.connections[name];
        connection.connection.connect();
        resolve(connection);
      }
    });

    if (!this.connections[name]?.promise) {
      this.connections[name].promise = connectionPromise;
    }
    return connectionPromise;
  }

  addExchange (name, type, options = {}, connectionName = DEFAULT) {
    if (typeof name === 'object') {
      options = name;
      options.connectionName = options.connectionName || type || connectionName;
    } else {
      options.name = name;
      options.type = type;
      options.connectionName = options.connectionName || connectionName;
    }
    return this.connections[options.connectionName].createExchange(options);
  }

  addQueue (name, options = {}, connectionName = DEFAULT) {
    options.name = name;
    if (options.subscribe && !this.hasHandles) {
      console.warn("Subscription to '" + name + "' was started without any handlers. This will result in lost messages!");
    }
    return this.connections[connectionName].createQueue(options, connectionName);
  }

  addSerializer (contentType, serializer) {
    serializers[contentType] = serializer;
  }

  batchAck () {
    signal.publish('ack', {});
  }

  bindExchange (source, target, keys, connectionName = DEFAULT) {
    return this.connections[connectionName].createBinding({ source, target, keys });
  }

  bindQueue (source, target, keys, connectionName = DEFAULT) {
    return this.connections[connectionName].createBinding(
      { source, target, keys, queue: true },
      connectionName
    );
  }

  async bulkPublish (set, connectionName = DEFAULT) {
    if (set.connectionName) {
      connectionName = set.connectionName;
    }
    if (!this.connections[connectionName]) {
      return Promise.reject(new Error(`BulkPublish failed - no connection ${connectionName} has been configured`));
    }

    const publish = (exchange, options) => {
      options.appId = options.appId || this.appId;
      options.timestamp = options.timestamp || Date.now();
      if (this.connections[connectionName] && this.connections[connectionName].options.publishTimeout) {
        options.connectionPublishTimeout = this.connections[connectionName].options.publishTimeout;
      }
      if (typeof options.body === 'number') {
        options.body = options.body.toString();
      }
      return exchange.publish(options)
        .then(
          () => options,
          err => { return { err, message: options }; }
        );
    };

    const exchangeNames = Array.isArray(set)
      ? set.reduce((acc, m) => {
        if (acc.indexOf(m.exchange) < 0) {
          acc.push(m.exchange);
        }
        return acc;
      }, [])
      : Object.keys(set);

    const exchanges1 = await this.onExchanges(exchangeNames, connectionName);
    if (!Array.isArray(set)) {
      const keys = Object.keys(set);
      return Promise.all(keys.map(exchangeName => {
        return Promise.all(set[exchangeName].map(message1 => {
          const exchange2 = exchanges1[exchangeName];
          if (exchange2) {
            return publish(exchange2, message1);
          } else {
            return Promise.reject(new Error(`Publish failed - no exchange ${exchangeName} on connection ${connectionName} is defined`));
          }
        }));
      }));
    } else {
      return Promise.all(set.map(message2 => {
        const exchange3 = exchanges1[message2.exchange];
        if (exchange3) {
          return publish(exchange3, message2);
        } else {
          return Promise.reject(new Error(`Publish failed - no exchange ${message2.exchange} on connection ${connectionName} is defined`));
        }
      }));
    }
  }

  clearAckInterval () {
    clearInterval(this.ackIntervalId);
  }

  closeAll (reset) {
    // COFFEE IS FOR CLOSERS
    const connectionNames = Object.keys(this.connections);
    const closers = connectionNames.map((connection) => this.close(connection, reset)
    );
    return Promise.all(closers);
  }

  close (connectionName = DEFAULT, reset = false) {
    const connection = this.connections[connectionName].connection;
    if (connection !== undefined && connection !== null) {
      if (reset) {
        this.connections[connectionName].reset();
      }
      delete this.configuring[connectionName];
      return connection.close(reset);
    } else {
      return Promise.resolve(true);
    }
  }

  deleteExchange (name, connectionName = DEFAULT) {
    return this.connections[connectionName].deleteExchange(name);
  }

  deleteQueue (name, connectionName = DEFAULT) {
    return this.connections[connectionName].deleteQueue(name);
  }

  getExchange (name, connectionName = DEFAULT) {
    return this.connections[connectionName].channels[`exchange:${name}`];
  }

  getQueue (name, connectionName = DEFAULT) {
    return this.connections[connectionName].channels[`queue:${name}`];
  }

  handle (messageType, handler, queueName, context) {
    this.hasHandles = true;
    let options;
    if (typeof messageType === 'string') {
      options = {
        type: messageType,
        queue: queueName || '*',
        context,
        autoNack: this.autoNack,
        handler
      };
    } else {
      options = messageType;
      options.autoNack = options.autoNack !== false;
      options.queue = options.queue || (options.type ? '*' : '#');
      options.handler = options.handler || handler;
    }
    const parts = [];
    if (options.queue === '#') {
      parts.push('#');
    } else {
      parts.push(options.queue.replace(/[.]/g, '-'));
      if (options.type !== '') {
        parts.push(options.type || '#');
      }
    }

    const target = parts.join('.');
    const subscription = dispatch.subscribe(target, options.handler.bind(options.context));
    if (options.autoNack) {
      subscription.catch(function (err, msg) {
        console.log("Handler for '" + target + "' failed with:", err.stack);
        msg.nack();
      });
    }
    subscription.remove = subscription.unsubscribe;
    return subscription;
  }

  ignoreHandlerErrors () {
    this.autoNack = false;
  }

  nackOnError () {
    this.autoNack = true;
  }

  nackUnhandled () {
    unhandledStrategies.onUnhandled = unhandledStrategies.nackOnUnhandled;
  }

  onUnhandled (handler) {
    unhandledStrategies.onUnhandled = unhandledStrategies.customOnUnhandled = handler;
  }

  rejectUnhandled () {
    unhandledStrategies.onUnhandled = unhandledStrategies.rejectOnUnhandled;
  }

  async onExchange (exchangeName, connectionName = DEFAULT) {
    const promises = [
      this.connections[connectionName].promise,
      this.connections[connectionName].promises[`exchange:${exchangeName}`]
    ];
    if (this.configuring[connectionName]) {
      promises.push(this.configuring[connectionName]);
    }
    await Promise.all(promises);
    return this.getExchange(exchangeName, connectionName);
  }

  async onExchanges (exchanges, connectionName = DEFAULT) {
    const connectionPromises = [this.connections[connectionName].promise];
    if (this.configuring[connectionName]) {
      connectionPromises.push(this.configuring[connectionName]);
    }
    const set = {};
    await Promise.all(connectionPromises);
    const exchangePromises = exchanges.map(exchangeName => this.connections[connectionName].promises[`exchange:${exchangeName}`]
      .then(() => {
        return { name: exchangeName, exchange: true };
      })
    );
    const list = await Promise.all(exchangePromises);
    list.forEach(item => {
      if (item && item.exchange) {
        const exchange = this.getExchange(item.name, connectionName);
        set[item.name] = exchange;
      }
    });
    return set;
  }

  onReturned (handler) {
    returnedStrategies.onReturned = returnedStrategies.customOnReturned = handler;
  }

  async publish (exchangeName, type, message, routingKey, correlationId, connectionName, sequenceNo) {
    const timestamp = Date.now();
    let options;
    if (typeof type === 'object') {
      options = type;
      connectionName = message || DEFAULT;
      options = Object.assign({
        appId: this.appId,
        timestamp,
        connectionName
      }, options);
      connectionName = options.connectionName;
    } else {
      connectionName = connectionName || message.connectionName || DEFAULT;
      options = {
        appId: this.appId,
        type,
        body: message,
        routingKey,
        correlationId,
        sequenceNo,
        timestamp,
        headers: {},
        connectionName
      };
    }
    if (!this.connections[connectionName]) {
      return Promise.reject(new Error(`Publish failed - no connection ${connectionName} has been configured`));
    }
    if (this.connections[connectionName] && this.connections[connectionName].options.publishTimeout) {
      options.connectionPublishTimeout = this.connections[connectionName].options.publishTimeout;
    }
    if (typeof options.body === 'number') {
      options.body = options.body.toString();
    }

    const exchange = await this.onExchange(exchangeName, connectionName);
    if (exchange) {
      return exchange.publish(options);
    } else {
      return Promise.reject(new Error(`Publish failed - no exchange ${exchangeName} on connection ${connectionName} is defined`));
    }
  }

  purgeQueue (queueName, connectionName = DEFAULT) {
    if (!this.connections[connectionName]) {
      return Promise.reject(new Error(`Queue purge failed - no connection ${connectionName} has been configured`));
    }
    return this.connections[connectionName].promise
      .then(() => {
        const queue = this.getQueue(queueName, connectionName);
        if (queue) {
          return queue.purge();
        } else {
          return Promise.reject(new Error(`Queue purge failed - no queue ${queueName} on connection ${connectionName} is defined`));
        }
      });
  }

  async request (exchangeName, options = {}, notify, connectionName = DEFAULT) {
    const requestId = uuid.v1();
    options.messageId = requestId;
    options.connectionName = options.connectionName || connectionName;

    if (!this.connections[options.connectionName]) {
      return Promise.reject(new Error(`Request failed - no connection ${options.connectionName} has been configured`));
    }

    const exchange = await this.onExchange(exchangeName, options.connectionName);
    const connection = this.connections[options.connectionName].options;
    const publishTimeout = options.timeout || exchange.publishTimeout || connection.publishTimeout || 500;
    const replyTimeout = options.replyTimeout || exchange.replyTimeout || connection.replyTimeout || (publishTimeout * 2);
    return await new Promise((resolve, reject) => {
      const timeout = setTimeout(function () {
        subscription.unsubscribe();
        reject(new Error('No reply received within the configured timeout of ' + replyTimeout + ' ms'));
      }, replyTimeout);
      const scatter = options.expect;
      let remaining = options.expect;
      const subscription = responses.subscribe(requestId, message1 => {
        const end = scatter
          ? --remaining <= 0
          : message1.properties.headers.sequence_end;
        if (end) {
          clearTimeout(timeout);
          if (!scatter || remaining === 0) {
            resolve(message1);
          }
          subscription.unsubscribe();
        } else if (notify) {
          notify(message1);
        }
      });
      this.publish(exchangeName, options);
    });
  }

  reset () {
    this.connections = {};
    this.configurations = {};
    this.configuring = {};
  }

  retry (connectionName = DEFAULT) {
    const config = this.configurations[connectionName];
    return this.configure(config);
  }

  setAckInterval (interval) {
    if (this.ackIntervalId) {
      this.clearAckInterval();
    }
    this.ackIntervalId = setInterval(this.batchAck, interval);
  }

  async shutdown () {
    await this.closeAll(true);
    this.clearAckInterval();
  }

  startSubscription (queueName, exclusive = false, connectionName = DEFAULT) {
    if (!this.hasHandles) {
      console.warn("Subscription to '" + queueName + "' was started without any handlers. This will result in lost messages!");
    }
    if (typeof exclusive === 'string') {
      connectionName = exclusive;
      exclusive = false;
    }
    const queue = this.getQueue(queueName, connectionName);
    if (queue) {
      return queue.subscribe(exclusive);
    } else {
      throw new Error("No queue named '" + queueName + "' for connection '" + connectionName + "'. Subscription failed.");
    }
  }

  stopSubscription (queueName, connectionName = DEFAULT) {
    const queue = this.getQueue(queueName, connectionName);
    if (queue) {
      queue.unsubscribe();
      return queue;
    } else {
      throw new Error("No queue named '" + queueName + "' for connection '" + connectionName + "'. Unsubscribe failed.");
    }
  }

  unbindExchange (source, target, keys, connectionName = DEFAULT) {
    return this.connections[connectionName].removeBinding({ source, target, keys });
  }

  unbindQueue (source, target, keys, connectionName = DEFAULT) {
    return this.connections[connectionName].removeBinding(
      { source, target, keys, queue: true },
      connectionName
    );
  }

  configure (config) {
    const emit = this.emit.bind(this);
    const configName = config.name || 'default';
    this.configurations[configName] = config;
    this.configuring[configName] = new Promise(function (resolve, reject) {
      function onExchangeError (connection, err) {
        confLog.error('Configuration of %s failed due to an error in one or more exchange settings: %s', connection.name, err);
        reject(err);
      }

      function onQueueError (connection, err) {
        confLog.error('Configuration of %s failed due to an error in one or more queue settings: %s', connection.name, err.stack);
        reject(err);
      }

      function onBindingError (connection, err) {
        confLog.error('Configuration of %s failed due to an error in one or more bindings: %s', connection.name, err.stack);
        reject(err);
      }

      function createExchanges (connection) {
        connection.configureExchanges(config.exchanges)
          .then(
            createQueues.bind(null, connection),
            onExchangeError.bind(null, connection)
          );
      }

      function createQueues (connection) {
        connection.configureQueues(config.queues)
          .then(
            createBindings.bind(null, connection),
            onQueueError.bind(null, connection)
          );
      }

      function createBindings (connection) {
        connection.configureBindings(config.bindings, connection.name)
          .then(
            finish.bind(null, connection),
            onBindingError.bind(null, connection)
          );
      }

      function finish (connection) {
        emit(connection.name + '.connection.configured', connection);
        resolve();
      }

      this.addConnection(config.connection)
        .then(
          function (connection) {
            createExchanges(connection);
            return connection;
          },
          reject
        );
    }.bind(this));
    return this.configuring[configName];
  }
}

Monologue.mixInto(Broker);

const broker = new Broker();

export default broker;
