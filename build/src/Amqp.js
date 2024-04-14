"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const uuid_1 = require("uuid");
const cloneDeep = require("lodash.clonedeep");
const amqplib_1 = require("amqplib");
const types_1 = require("./types");
const constants_1 = require("./constants");
class Amqp {
    constructor(RED, node, config) {
        this.RED = RED;
        this.node = node;
        this.config = {
            name: config.name,
            broker: config.broker,
            prefetch: config.prefetch,
            reconnectOnError: config.reconnectOnError,
            noAck: config.noAck,
            exchange: {
                name: config.exchangeName,
                type: config.exchangeType,
                routingKey: config.exchangeRoutingKey,
                durable: config.exchangeDurable,
            },
            queue: {
                name: config.queueName,
                exclusive: config.queueExclusive,
                durable: config.queueDurable,
                autoDelete: config.queueAutoDelete,
            },
            amqpProperties: this.parseJson(config.amqpProperties),
            headers: this.parseJson(config.headers),
            outputs: config.outputs,
            rpcTimeout: config.rpcTimeoutMilliseconds,
        };
    }
    async connect() {
        const { broker } = this.config;
        // wtf happened to the types?
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore
        this.broker = this.RED.nodes.getNode(broker);
        const brokerUrl = this.getBrokerUrl(this.broker);
        this.connection = await amqplib_1.connect(brokerUrl, { heartbeat: 2 });
        /* istanbul ignore next */
        this.connection.on('error', (e) => {
            // Set node to disconnected status
            this.node.status(constants_1.NODE_STATUS.Disconnected);
        });
        /* istanbul ignore next */
        this.connection.on('close', () => {
            this.node.status(constants_1.NODE_STATUS.Disconnected);
            this.node.log(`AMQP Connection closed`);
        });
        return this.connection;
    }
    async initialize() {
        await this.createChannel();
        await this.assertExchange();
        return this.channel;
    }
    async consume() {
        try {
            const { noAck } = this.config;
            await this.assertQueue();
            this.bindQueue();
            await this.channel.consume(this.q.queue, amqpMessage => {
                const msg = this.assembleMessage(amqpMessage);
                this.node.send(msg);
                /* istanbul ignore else */
                if (!noAck && !this.isManualAck()) {
                    this.ack(msg);
                }
            }, { noAck });
        }
        catch (e) {
            this.node.error(`Could not consume message: ${e}`);
        }
    }
    setRoutingKey(newRoutingKey) {
        this.config.exchange.routingKey = newRoutingKey;
    }
    ack(msg) {
        var _a;
        const allUpTo = !!((_a = msg.manualAck) === null || _a === void 0 ? void 0 : _a.allUpTo);
        this.channel.ack(msg, allUpTo);
    }
    ackAll() {
        this.channel.ackAll();
    }
    nack(msg) {
        var _a, _b, _c;
        const allUpTo = !!((_a = msg.manualAck) === null || _a === void 0 ? void 0 : _a.allUpTo);
        const requeue = (_c = (_b = msg.manualAck) === null || _b === void 0 ? void 0 : _b.requeue) !== null && _c !== void 0 ? _c : true;
        this.channel.nack(msg, allUpTo, requeue);
    }
    nackAll(msg) {
        var _a, _b;
        const requeue = (_b = (_a = msg.manualAck) === null || _a === void 0 ? void 0 : _a.requeue) !== null && _b !== void 0 ? _b : true;
        this.channel.nackAll(requeue);
    }
    reject(msg) {
        var _a, _b;
        const requeue = (_b = (_a = msg.manualAck) === null || _a === void 0 ? void 0 : _a.requeue) !== null && _b !== void 0 ? _b : true;
        this.channel.reject(msg, requeue);
    }
    async publish(msg, properties) {
        this.parseRoutingKeys().forEach(async (routingKey) => {
            this.handlePublish(this.config, msg, properties, routingKey);
        });
    }
    async handlePublish(config, msg, properties, routingKey) {
        var _a, _b;
        const { exchange: { name }, outputs: rpcRequested, } = config;
        try {
            let correlationId = '';
            let replyTo = '';
            if (rpcRequested) {
                // Send request for remote procedure call
                correlationId =
                    (properties === null || properties === void 0 ? void 0 : properties.correlationId) ||
                        ((_a = this.config.amqpProperties) === null || _a === void 0 ? void 0 : _a.correlationId) ||
                        uuid_1.v4();
                replyTo =
                    (properties === null || properties === void 0 ? void 0 : properties.replyTo) || ((_b = this.config.amqpProperties) === null || _b === void 0 ? void 0 : _b.replyTo) || uuid_1.v4();
                await this.handleRemoteProcedureCall(correlationId, replyTo);
            }
            const options = Object.assign(Object.assign({ correlationId,
                replyTo }, this.config.amqpProperties), properties);
            // when the name field is empty, publish just like the sendToQueue method;
            // see https://amqp-node.github.io/amqplib/channel_api.html#channel_publish
            this.channel.publish(name, routingKey, Buffer.from(msg), options);
        }
        catch (e) {
            this.node.error(`Could not publish message: ${e}`);
        }
    }
    getRpcConfig(replyTo) {
        const rpcConfig = cloneDeep(this.config);
        rpcConfig.exchange.name = '';
        rpcConfig.queue.name = replyTo;
        rpcConfig.queue.autoDelete = true;
        rpcConfig.queue.exclusive = true;
        rpcConfig.queue.durable = false;
        rpcConfig.noAck = true;
        return rpcConfig;
    }
    async handleRemoteProcedureCall(correlationId, replyTo) {
        const rpcConfig = this.getRpcConfig(replyTo);
        try {
            // If we try to delete a queue that's already deleted
            // bad things will happen.
            let rpcQueueHasBeenDeleted = false;
            let additionalErrorMessaging = '';
            /************************************
             * assert queue and set up consumer
             ************************************/
            const queueName = await this.assertQueue(rpcConfig);
            await this.channel.consume(queueName, async (amqpMessage) => {
                if (amqpMessage) {
                    const msg = this.assembleMessage(amqpMessage);
                    if (msg.properties.correlationId === correlationId) {
                        this.node.send(msg);
                        /* istanbul ignore else */
                        if (!rpcQueueHasBeenDeleted) {
                            await this.channel.deleteQueue(queueName);
                            rpcQueueHasBeenDeleted = true;
                        }
                    }
                    else {
                        additionalErrorMessaging += ` Correlation ids do not match. Expecting: ${correlationId}, received: ${msg.properties.correlationId}`;
                    }
                }
            }, { noAck: rpcConfig.noAck });
            /****************************************
             * Check if RPC has timed out and handle
             ****************************************/
            setTimeout(async () => {
                try {
                    if (!rpcQueueHasBeenDeleted) {
                        this.node.send({
                            payload: {
                                message: `Timeout while waiting for RPC response.${additionalErrorMessaging}`,
                                config: rpcConfig,
                            },
                        });
                        await this.channel.deleteQueue(queueName);
                    }
                }
                catch (e) {
                    // TODO: Keep an eye on this
                    // This might close the whole channel
                    this.node.error(`Error trying to cancel RPC consumer: ${e}`);
                }
            }, rpcConfig.rpcTimeout || 3000);
        }
        catch (e) {
            this.node.error(`Could not consume RPC message: ${e}`);
        }
    }
    async close() {
        var _a;
        const { name: exchangeName } = this.config.exchange;
        const queueName = (_a = this.q) === null || _a === void 0 ? void 0 : _a.queue;
        try {
            /* istanbul ignore else */
            if (exchangeName && queueName) {
                const routingKeys = this.parseRoutingKeys();
                try {
                    for (let x = 0; x < routingKeys.length; x++) {
                        await this.channel.unbindQueue(queueName, exchangeName, routingKeys[x]);
                    }
                }
                catch (e) {
                    /* istanbul ignore next */
                    console.error('Error unbinding queue: ', e.message);
                }
            }
            await this.channel.close();
            await this.connection.close();
        }
        catch (e) { } // Need to catch here but nothing further is necessary
    }
    async createChannel() {
        const { prefetch } = this.config;
        this.channel = await this.connection.createChannel();
        this.channel.prefetch(Number(prefetch));
        /* istanbul ignore next */
        this.channel.on('error', (e) => {
            // Set node to disconnected status
            this.node.status(constants_1.NODE_STATUS.Disconnected);
            this.node.error(`AMQP Connection Error ${e}`, { payload: { error: e, source: 'Amqp' } });
        });
        return this.channel;
    }
    async assertExchange() {
        const { name, type, durable } = this.config.exchange;
        /* istanbul ignore else */
        if (name) {
            await this.channel.assertExchange(name, type, {
                durable,
            });
        }
    }
    async assertQueue(configParams) {
        const { queue } = configParams || this.config;
        const { name, exclusive, durable, autoDelete } = queue;
        this.q = await this.channel.assertQueue(name, {
            exclusive,
            durable,
            autoDelete,
        });
        return name;
    }
    async bindQueue(configParams) {
        const { name, type, routingKey } = (configParams === null || configParams === void 0 ? void 0 : configParams.exchange) || this.config.exchange;
        const { headers } = (configParams === null || configParams === void 0 ? void 0 : configParams.amqpProperties) || this.config;
        if (this.canHaveRoutingKey(type)) {
            /* istanbul ignore else */
            if (name) {
                this.parseRoutingKeys(routingKey).forEach(async (routingKey) => {
                    await this.channel.bindQueue(this.q.queue, name, routingKey);
                });
            }
        }
        if (type === types_1.ExchangeType.Fanout) {
            await this.channel.bindQueue(this.q.queue, name, '');
        }
        if (type === types_1.ExchangeType.Headers) {
            await this.channel.bindQueue(this.q.queue, name, '', headers);
        }
    }
    canHaveRoutingKey(type) {
        return type === types_1.ExchangeType.Direct || type === types_1.ExchangeType.Topic;
    }
    getBrokerUrl(broker) {
        let url = '';
        if (broker) {
            const { host, port, vhost, tls, credsFromSettings, credentials } = broker;
            const { username, password } = credsFromSettings
                ? this.getCredsFromSettings()
                : credentials;
            const protocol = tls ? /* istanbul ignore next */ 'amqps' : 'amqp';
            url = `${protocol}://${encodeURIComponent(username)}:${encodeURIComponent(password)}@${host}:${port}/${vhost}`;
        }
        return url;
    }
    getCredsFromSettings() {
        return {
            // eslint-disable-next-line @typescript-eslint/ban-ts-comment
            // @ts-ignore
            username: this.RED.settings.MW_CONTRIB_AMQP_USERNAME,
            // eslint-disable-next-line @typescript-eslint/ban-ts-comment
            // @ts-ignore
            password: this.RED.settings.MW_CONTRIB_AMQP_PASSWORD,
        };
    }
    parseRoutingKeys(routingKeyArg) {
        var _a;
        const routingKey = routingKeyArg || this.config.exchange.routingKey || ((_a = this.q) === null || _a === void 0 ? void 0 : _a.queue) || '';
        const keys = routingKey === null || routingKey === void 0 ? void 0 : routingKey.split(',').map(key => key.trim());
        return keys;
    }
    assembleMessage(amqpMessage) {
        const payload = this.parseJson(amqpMessage.content.toString());
        return Object.assign(Object.assign({}, amqpMessage), { payload });
    }
    isManualAck() {
        return this.node.type === types_1.NodeType.AmqpInManualAck;
    }
    parseJson(jsonInput) {
        let output;
        try {
            output = JSON.parse(jsonInput);
        }
        catch (_a) {
            output = jsonInput;
        }
        return output;
    }
}
exports.default = Amqp;
