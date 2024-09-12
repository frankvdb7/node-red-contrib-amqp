"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const constants_1 = require("../constants");
const types_1 = require("../types");
const Amqp_1 = require("../Amqp");
module.exports = function (RED) {
    function AmqpInManualAck(config) {
        let reconnectTimeout;
        let reconnect = null;
        let connection = null;
        let channel = null;
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
        const inputListener = async (msg, _, done) => {
            // handle manualAck
            if (msg.manualAck) {
                const ackMode = msg.manualAck.ackMode;
                switch (ackMode) {
                    case types_1.ManualAckType.AckAll:
                        amqp.ackAll();
                        break;
                    case types_1.ManualAckType.Nack:
                        amqp.nack(msg);
                        break;
                    case types_1.ManualAckType.NackAll:
                        amqp.nackAll(msg);
                        break;
                    case types_1.ManualAckType.Reject:
                        amqp.reject(msg);
                        break;
                    case types_1.ManualAckType.Ack:
                    default:
                        amqp.ack(msg);
                        break;
                }
            }
            else {
                amqp.ack(msg);
            }
            // handle manual reconnect
            if (msg.payload && msg.payload.reconnectCall && typeof reconnect === 'function') {
                await reconnect();
                done && done();
            }
            else {
                done && done();
            }
        };
        // receive input reconnectCall
        this.on('input', inputListener);
        // When the server goes down
        this.on('close', async (done) => {
            await amqp.close();
            done && done();
        });
        async function initializeNode(nodeIns) {
            reconnect = async () => {
                // check the channel and clear all the event listener
                if (channel && channel.removeAllListeners) {
                    channel.removeAllListeners();
                    channel.close();
                    channel = null;
                }
                // check the connection and clear all the event listener
                if (connection && connection.removeAllListeners) {
                    connection.removeAllListeners();
                    connection.close();
                    connection = null;
                }
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
                    const channel = await amqp.initialize();
                    await amqp.consume();
                    // When the connection goes down
                    connection.on('close', async (e) => {
                        e && (await reconnect());
                    });
                    // When the connection goes down
                    connection.on('error', async (e) => {
                        e && reconnectOnError && (await reconnect());
                        nodeIns.error(`Connection error ${e}`, { payload: { error: e, location: types_1.ErrorLocationEnum.ConnectionErrorEvent } });
                    });
                    // When the channel goes down
                    channel.on('error', async (e) => {
                        e && reconnectOnError && (await reconnect());
                        nodeIns.error(`Channel error ${e}`, { payload: { error: e, location: types_1.ErrorLocationEnum.ChannelErrorEvent } });
                    });
                    nodeIns.status(constants_1.NODE_STATUS.Connected);
                }
            }
            catch (e) {
                if (e.code === types_1.ErrorType.ConnectionRefused || e.isOperational) {
                    reconnectOnError && (await reconnect());
                }
                else if (e.code === types_1.ErrorType.InvalidLogin) {
                    nodeIns.status(constants_1.NODE_STATUS.Invalid);
                    nodeIns.error(`AmqpInManualAck() Could not connect to broker ${e}`, { payload: { error: e, location: types_1.ErrorLocationEnum.ConnectError } });
                }
                else {
                    nodeIns.status(constants_1.NODE_STATUS.Error);
                    nodeIns.error(`AmqpInManualAck() ${e}`, { payload: { error: e, location: types_1.ErrorLocationEnum.ConnectError } });
                }
            }
        }
        // call
        initializeNode(this);
    }
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    RED.nodes.registerType(types_1.NodeType.AmqpInManualAck, AmqpInManualAck);
};
//# sourceMappingURL=amqp-in-manual-ack.js.map