"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.NODE_STATUS = void 0;
exports.NODE_STATUS = Object.freeze({
    Connected: { fill: 'green', shape: 'dot', text: 'Connected' },
    Disconnected: { fill: 'grey', shape: 'ring', text: 'Disconnected' },
    Error: { fill: 'red', shape: 'dot', text: 'Error' },
    Invalid: { fill: 'red', shape: 'ring', text: 'Unable to connect' },
});
