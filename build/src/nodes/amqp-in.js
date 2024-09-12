"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const constants_1 = require("../constants");
const types_1 = require("../types");
const Amqp_1 = require("../Amqp");
module.exports = function (RED) {
    function AmqpIn(config) {
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
        // When the node is re-deployed
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
                connection = await amqp.connect();
                // istanbul ignore else
                if (connection) {
                    channel = await amqp.initialize();
                    await amqp.consume();
                    // When the connection goes down
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
                        try {
                            //await reconnect(); // why not?
                        }
                        catch (e) {
                            nodeIns.error(`Channel error: ${JSON.stringify(e)}}`, { payload: { error: e, location: types_1.ErrorLocationEnum.ChannelErrorEvent } });
                        }
                    });
                    // When the channel goes down
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
                    nodeIns.error(`AmqpIn() Could not connect to broker ${e}`, { payload: { error: e, location: types_1.ErrorLocationEnum.ConnectError } });
                }
                else {
                    nodeIns.status(constants_1.NODE_STATUS.Error);
                    nodeIns.error(`AmqpIn() ${JSON.stringify(e)}`, { payload: { error: e, source: 'ConnectionError' } });
                }
            }
        }
        // call
        initializeNode(this);
    }
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    RED.nodes.registerType(types_1.NodeType.AmqpIn, AmqpIn);
};
//# sourceMappingURL=amqp-in.js.map