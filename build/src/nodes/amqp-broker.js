"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
module.exports = function (RED) {
    function AmqpBroker(n) {
        // wtf happened to the types?
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore
        RED.nodes.createNode(this, n);
        this.name = n.name;
        this.host = n.host;
        this.port = n.port;
        this.tls = n.tls;
        this.vhost = n.vhost;
        this.credsFromSettings = n.credsFromSettings;
    }
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    RED.nodes.registerType('amqp-broker', AmqpBroker, {
        credentials: {
            username: { type: 'text' },
            password: { type: 'password' },
        },
    });
};
