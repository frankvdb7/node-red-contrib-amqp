"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const constants_1 = require("../constants");
const types_1 = require("../types");
const Amqp_1 = require("../Amqp");
module.exports = function (RED) {
    function AmqpOut(config) {
        let reconnectTimeout;
        let reconnect = null;
        let connection = null;
        let channel = null;
        let me = this;
        RED.events.once('flows:stopped', () => {
            clearTimeout(reconnectTimeout);
        });
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore
        RED.nodes.createNode(this, config);
        this.status(constants_1.NODE_STATUS.Disconnected);
        const configAmqp = config;
        const amqp = new Amqp_1.default(RED, this, configAmqp);
        const reconnectOnError = configAmqp.reconnectOnError;
        // handle input event;
        const inputListener = async (msg, _, done) => {
            var _a;
            const { payload, routingKey, properties: msgProperties } = msg;
            const { exchangeRoutingKey, exchangeRoutingKeyType, amqpProperties, } = config;
            // message properties override config properties
            let properties;
            try {
                properties = Object.assign(Object.assign({}, JSON.parse(amqpProperties)), msgProperties);
            }
            catch (e) {
                properties = msgProperties;
            }
            switch (exchangeRoutingKeyType) {
                case 'msg':
                case 'flow':
                case 'global':
                    amqp.setRoutingKey(RED.util.evaluateNodeProperty(exchangeRoutingKey, exchangeRoutingKeyType, this, msg));
                    break;
                case 'jsonata':
                    amqp.setRoutingKey(RED.util.evaluateJSONataExpression(RED.util.prepareJSONataExpression(exchangeRoutingKey, this), msg));
                    break;
                case 'str':
                default:
                    if (routingKey) {
                        // if incoming payload contains a routingKey value
                        // override our string value with it.
                        // Superfluous (and possibly confusing) at this point
                        // but keeping it to retain backwards compatibility
                        amqp.setRoutingKey(routingKey);
                    }
                    break;
            }
            if (!!((_a = properties === null || properties === void 0 ? void 0 : properties.headers) === null || _a === void 0 ? void 0 : _a.doNotStringifyPayload)) {
                amqp.publish(payload, properties);
            }
            else {
                amqp.publish(JSON.stringify(payload), properties);
            }
            done && done();
        };
        this.on('input', inputListener);
        // When the node is re-deployed
        this.on('close', async (done) => {
            await amqp.close();
            done && done();
        });
        async function initializeNode(nodeIns) {
            reconnect = async () => {
                // check the channel and clear all the event listener
                try {
                    if (channel && channel.removeAllListeners) {
                        channel.removeAllListeners();
                        channel.close()
                            .catch(err => {
                            me.error('Error closing channel:', err);
                        });
                        //channel = null;
                    }
                }
                catch (error) {
                    // catch and suppress error
                    me.error('Error occurred:', error);
                }
                channel = null;
                try {
                    // check the connection and clear all the event listener
                    if (connection && connection.removeAllListeners) {
                        connection.removeAllListeners();
                        connection.close()
                            .catch(err => {
                            me.error('Error closing connection:', err);
                        });
                        //connection = null;
                    }
                }
                catch (error) {
                    // catch and suppress error
                    me.error('Error occurred:', error);
                }
                connection = null;
                // always clear timer before set it;
                clearTimeout(reconnectTimeout);
                reconnectTimeout = setTimeout(() => {
                    try {
                        initializeNode(nodeIns);
                    }
                    catch (e) {
                        reconnect();
                    }
                }, 2000);
            };
            try {
                const connection = await amqp.connect();
                // istanbul ignore else
                if (connection) {
                    channel = await amqp.initialize();
                    // When the server goes down
                    connection.on('close', async (e) => {
                        e && (await reconnect());
                    });
                    // When the connection goes down
                    connection.on('error', async (e) => {
                        reconnectOnError && (await reconnect());
                        nodeIns.error(`Connection error ${e}`, { payload: { error: e, location: types_1.ErrorLocationEnum.ConnectionErrorEvent } });
                    });
                    // When the channel goes down
                    channel.on('close', async () => {
                        await reconnect();
                    });
                    // When the channel error occur
                    channel.on('error', async (e) => {
                        reconnectOnError && (await reconnect());
                        nodeIns.error(`Channel error ${e}`, { payload: { error: e, location: types_1.ErrorLocationEnum.ChannelErrorEvent } });
                    });
                    nodeIns.status(constants_1.NODE_STATUS.Connected);
                }
            }
            catch (e) {
                reconnectOnError && (await reconnect());
                if (e.code === types_1.ErrorType.InvalidLogin) {
                    nodeIns.status(constants_1.NODE_STATUS.Invalid);
                    nodeIns.error(`AmqpOut() Could not connect to broker ${e}`, { payload: { error: e, location: types_1.ErrorLocationEnum.ConnectError } });
                }
                else {
                    nodeIns.status(constants_1.NODE_STATUS.Error);
                    nodeIns.error(`AmqpOut() ${e}`, { payload: { error: e, location: types_1.ErrorLocationEnum.ConnectError } });
                }
            }
        }
        // call
        initializeNode(this);
    }
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    RED.nodes.registerType(types_1.NodeType.AmqpOut, AmqpOut);
};
