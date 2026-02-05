/* eslint-disable @typescript-eslint/no-var-requires */
/* eslint-disable @typescript-eslint/ban-ts-comment */
export {}
const { expect } = require('chai')
const sinon = require('sinon')
const Amqp = require('../../src/Amqp').default
const { ErrorType, ManualAckType, NodeType } = require('../../src/types')
const {
  CustomError,
  amqpInManualAckFlowFixture,
  credentialsFixture,
} = require('../doubles')
const { NODE_STATUS } = require('../../src/constants')
const helper = require('node-red-node-test-helper')
const amqpInManualAck = require('../../src/nodes/amqp-in-manual-ack')
const amqpBroker = require('../../src/nodes/amqp-broker')

helper.init(require.resolve('node-red'))

describe('amqp-in-manual-ack Node', () => {
  beforeEach(function (done) {
    helper.startServer(done)
  })

  afterEach(function (done) {
    helper.unload()
    helper.stopServer(done)
    sinon.restore()
  })

  it('should be loaded', done => {
    sinon.stub(Amqp.prototype, 'connect')
    const flow = [
      { id: 'n1', type: NodeType.AmqpInManualAck, name: 'test name' },
    ]
    helper.load(amqpInManualAck, flow, () => {
      const n1 = helper.getNode('n1')
      n1.should.have.property('name', 'test name')
      done()
    })
  })

  it('should connect to the server', function (done) {
    // @ts-ignore
    Amqp.prototype.channel = {
      unbindQueue: (): null => null,
      close: (): null => null,
      ack: (): null => null,
    }
    // @ts-ignore
    Amqp.prototype.connection = {
      close: (): null => null,
    }
    const connectStub = sinon
      .stub(Amqp.prototype, 'connect')
      // @ts-ignore
      .resolves(true)
    const initializeStub = sinon.stub(Amqp.prototype, 'initialize')

    helper.load(
      [amqpInManualAck, amqpBroker],
      amqpInManualAckFlowFixture,
      credentialsFixture,
      async function () {
        expect(connectStub.calledOnce).to.be.true
        const amqpInManualAckNode = helper.getNode('n1')
        amqpInManualAckNode.close(true)
        done()
      },
    )
  })

  describe('Manual Acknowledgment', () => {
    let ackStub, ackAllStub, nackStub, nackAllStub, rejectStub;

    beforeEach(() => {
      ackStub = sinon.stub(Amqp.prototype, 'ack');
      ackAllStub = sinon.stub(Amqp.prototype, 'ackAll');
      nackStub = sinon.stub(Amqp.prototype, 'nack');
      nackAllStub = sinon.stub(Amqp.prototype, 'nackAll');
      rejectStub = sinon.stub(Amqp.prototype, 'reject');
    });

    it('should ack a message by default', done => {
      helper.load([amqpInManualAck, amqpBroker], amqpInManualAckFlowFixture, credentialsFixture, () => {
        const n1 = helper.getNode('n1');
        n1.receive({ payload: 'test' });
        expect(ackStub.calledOnce).to.be.true;
        done();
      });
    });

    it('should ack a message', done => {
      helper.load([amqpInManualAck, amqpBroker], amqpInManualAckFlowFixture, credentialsFixture, () => {
        const n1 = helper.getNode('n1');
        n1.receive({ payload: 'test', manualAck: { ackMode: ManualAckType.Ack } });
        expect(ackStub.calledOnce).to.be.true;
        done();
      });
    });

    it('should ack all messages', done => {
      helper.load([amqpInManualAck, amqpBroker], amqpInManualAckFlowFixture, credentialsFixture, () => {
        const n1 = helper.getNode('n1');
        n1.receive({ payload: 'test', manualAck: { ackMode: ManualAckType.AckAll } });
        expect(ackAllStub.calledOnce).to.be.true;
        done();
      });
    });

    it('should nack a message', done => {
      helper.load([amqpInManualAck, amqpBroker], amqpInManualAckFlowFixture, credentialsFixture, () => {
        const n1 = helper.getNode('n1');
        n1.receive({ payload: 'test', manualAck: { ackMode: ManualAckType.Nack } });
        expect(nackStub.calledOnce).to.be.true;
        done();
      });
    });

    it('should nack all messages', done => {
      helper.load([amqpInManualAck, amqpBroker], amqpInManualAckFlowFixture, credentialsFixture, () => {
        const n1 = helper.getNode('n1');
        n1.receive({ payload: 'test', manualAck: { ackMode: ManualAckType.NackAll } });
        expect(nackAllStub.calledOnce).to.be.true;
        done();
      });
    });

    it('should reject a message', done => {
      helper.load([amqpInManualAck, amqpBroker], amqpInManualAckFlowFixture, credentialsFixture, () => {
        const n1 = helper.getNode('n1');
        n1.receive({ payload: 'test', manualAck: { ackMode: ManualAckType.Reject } });
        expect(rejectStub.calledOnce).to.be.true;
        done();
      });
    });
  });

  it('tries to connect but the broker is down', function (done) {
    const connectStub = sinon
      .stub(Amqp.prototype, 'connect')
      .throws(new CustomError(ErrorType.ConnectionRefused))
    helper.load(
      [amqpInManualAck, amqpBroker],
      amqpInManualAckFlowFixture,
      credentialsFixture,
      function () {
        expect(connectStub).to.throw()
        done()
      },
    )
  })

  it('catches an invalid login exception', function (done) {
    const connectStub = sinon
      .stub(Amqp.prototype, 'connect')
      .throws(new CustomError(ErrorType.InvalidLogin))
    helper.load(
      [amqpInManualAck, amqpBroker],
      amqpInManualAckFlowFixture,
      credentialsFixture,
      function () {
        expect(connectStub).to.throw()
        done()
      },
    )
  })

  it('detects invalid login from message text', function (done) {
    const err = new CustomError(ErrorType.ConnectionRefused, 'ACCESS_REFUSED - Login failed')
    const connectStub = sinon.stub(Amqp.prototype, 'connect').throws(err)
    helper.load(
      [amqpInManualAck, amqpBroker],
      amqpInManualAckFlowFixture,
      credentialsFixture,
      function () {
        const amqpInManualAckNode = helper.getNode('n1')
        amqpInManualAckNode.on('call:status', call => {
          if (call.args[0].text === NODE_STATUS.Invalid.text) {
            try {
              expect(call.args[0]).to.deep.equal(NODE_STATUS.Invalid)
              expect(connectStub).to.throw()
              done()
            } catch (err) {
              done(err)
            }
          }
        })
      },
    )
  })

  it('handles dns lookup failures', function (done) {
    const connectStub = sinon
      .stub(Amqp.prototype, 'connect')
      .throws(new CustomError(ErrorType.DnsResolve))

    helper.load(
      [amqpInManualAck, amqpBroker],
      amqpInManualAckFlowFixture,
      credentialsFixture,
      function () {
        expect(connectStub).to.throw()
        done()
      },
    )
  })

  it('catches a generic exception', function (done) {
    const connectStub = sinon.stub(Amqp.prototype, 'connect').throws()
    helper.load(
      [amqpInManualAck, amqpBroker],
      amqpInManualAckFlowFixture,
      credentialsFixture,
      function () {
        expect(connectStub).to.throw()
        done()
      },
    )
  })

  it('handles non-object connect errors', function (done) {
    const connectStub = sinon.stub(Amqp.prototype, 'connect').throws('boom')
    helper.load(
      [amqpInManualAck, amqpBroker],
      amqpInManualAckFlowFixture,
      credentialsFixture,
      function () {
        const amqpInManualAckNode = helper.getNode('n1')
        amqpInManualAckNode.on('call:status', call => {
          if (call.args[0].text === NODE_STATUS.Error.text) {
            try {
              expect(call.args[0]).to.deep.equal(NODE_STATUS.Error)
              expect(connectStub).to.throw()
              done()
            } catch (err) {
              done(err)
            }
          }
        })
      },
    )
  })

  it('does not register flows:stopped listener', function (done) {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub() }
    sinon
      .stub(Amqp.prototype, 'connect')
      .resolves(connectionMock as any)
    sinon
      .stub(Amqp.prototype, 'initialize')
      .resolves(channelMock as any)
    sinon.stub(Amqp.prototype, 'consume').resolves()

    helper.load(
      [amqpInManualAck, amqpBroker],
      amqpInManualAckFlowFixture,
      credentialsFixture,
      function () {
        expect(helper._events.listenerCount('flows:stopped')).to.equal(0)
        const node = helper.getNode('n1')
        node.close()
        done()
      },
    )
  })

  it('should reconnect on input message', done => {
    const connectStub = sinon.stub(Amqp.prototype, 'connect')
    const closeStub = sinon.stub(Amqp.prototype, 'close')
    const flow = [
      { id: 'n1', type: NodeType.AmqpInManualAck, name: 'test name', broker: 'b1', wires: [[]] },
      { id: 'b1', type: 'amqp-broker', name: 'test broker' },
    ]
    helper.load([amqpInManualAck, amqpBroker], flow, credentialsFixture, () => {
      const n1 = helper.getNode('n1')
      n1.receive({ payload: { reconnectCall: true }})
      expect(closeStub.calledOnce).to.be.true
      done()
    })
  })

  it('handles connection close', async function () {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub() }
    sinon
      .stub(Amqp.prototype, 'connect')
      .resolves(connectionMock as any)
    sinon
      .stub(Amqp.prototype, 'initialize')
      .resolves(channelMock as any)
    const closeStub = sinon.stub(Amqp.prototype, 'close')

    await helper.load(
      [amqpInManualAck, amqpBroker],
      amqpInManualAckFlowFixture,
      credentialsFixture,
    )
    // Get the 'on' callback for connection close
    const onCallback = connectionMock.on.withArgs('close').getCall(0).args[1]
    onCallback('connection closed')
    expect(closeStub.calledOnce).to.be.true
  })

  it('should handle connection errors', function (done) {
    const flow = [
      { id: 'n1', type: 'amqp-in-manual-ack', name: 'test name', broker: 'b1' },
      { id: 'b1', type: 'amqp-broker', name: 'test broker' }
    ];
    helper.load([amqpInManualAck, amqpBroker], flow, function () {
      const amqpInNode = helper.getNode('n1');
      const brokerNode = helper.getNode('b1');
      brokerNode.on('amqp-in-error', (err) => {
        try {
          expect(err.message).to.equal('Connection error');
          done();
        } catch (e) {
          done(e);
        }
      });
      brokerNode.emit('amqp-in-error', new Error('Connection error'));
    });
  });

  it('does not reconnect on connection error when reconnectOnError is false', async function () {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() };
    const channelMock = { on: sinon.stub(), off: sinon.stub() };
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any);
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any);
    const closeStub = sinon.stub(Amqp.prototype, 'close');

    const flow = JSON.parse(JSON.stringify(amqpInManualAckFlowFixture));
    flow[1].reconnectOnError = false;

    await helper.load([amqpInManualAck, amqpBroker], flow, credentialsFixture)
    const onCallback = connectionMock.on.withArgs('error').getCall(0).args[1]
    onCallback('connection error')
    expect(closeStub.called).to.be.false
  });

  it('does not reconnect on initialization failure when reconnectOnError is false', async function () {
    const error = new Error('Initial connection failed');
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (error as any).code = ErrorType.ConnectionRefused;

    const connectStub = sinon.stub(Amqp.prototype, 'connect').rejects(error);
    const initChannelMock = { on: sinon.stub(), off: sinon.stub() }
    sinon.stub(Amqp.prototype, 'initialize').resolves(initChannelMock as any)
    sinon.stub(Amqp.prototype, 'consume').resolves()

    await helper.load([amqpInManualAck, amqpBroker], amqpInManualAckFlowFixture, credentialsFixture);
    expect(connectStub.callCount).to.equal(1);
  });
})
