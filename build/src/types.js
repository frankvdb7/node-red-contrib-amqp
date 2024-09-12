"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ErrorLocationEnum = exports.DefaultExchangeName = exports.ExchangeType = exports.NodeType = exports.ErrorType = exports.ManualAckType = void 0;
var ManualAckType;
(function (ManualAckType) {
    ManualAckType["Ack"] = "ack";
    ManualAckType["AckAll"] = "ackAll";
    ManualAckType["Nack"] = "nack";
    ManualAckType["NackAll"] = "nackAll";
    ManualAckType["Reject"] = "reject";
})(ManualAckType = exports.ManualAckType || (exports.ManualAckType = {}));
var ErrorType;
(function (ErrorType) {
    ErrorType["InvalidLogin"] = "ENOTFOUND";
    ErrorType["ConnectionRefused"] = "ECONNREFUSED";
})(ErrorType = exports.ErrorType || (exports.ErrorType = {}));
var NodeType;
(function (NodeType) {
    NodeType["AmqpIn"] = "amqp-in";
    NodeType["AmqpOut"] = "amqp-out";
    NodeType["AmqpInManualAck"] = "amqp-in-manual-ack";
})(NodeType = exports.NodeType || (exports.NodeType = {}));
var ExchangeType;
(function (ExchangeType) {
    ExchangeType["Direct"] = "direct";
    ExchangeType["Fanout"] = "fanout";
    ExchangeType["Topic"] = "topic";
    ExchangeType["Headers"] = "headers";
})(ExchangeType = exports.ExchangeType || (exports.ExchangeType = {}));
var DefaultExchangeName;
(function (DefaultExchangeName) {
    DefaultExchangeName["Direct"] = "amq.direct";
    DefaultExchangeName["Fanout"] = "amq.fanout";
    DefaultExchangeName["Topic"] = "amq.topic";
    DefaultExchangeName["Headers"] = "amq.headers";
})(DefaultExchangeName = exports.DefaultExchangeName || (exports.DefaultExchangeName = {}));
var ErrorLocationEnum;
(function (ErrorLocationEnum) {
    ErrorLocationEnum["ConnectError"] = "ConnectError";
    ErrorLocationEnum["ConnectionErrorEvent"] = "ConnectionErrorEvent";
    ErrorLocationEnum["ChannelErrorEvent"] = "ChannelErrorEvent";
})(ErrorLocationEnum = exports.ErrorLocationEnum || (exports.ErrorLocationEnum = {}));
//# sourceMappingURL=types.js.map