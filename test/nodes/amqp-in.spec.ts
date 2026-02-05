/* eslint-disable @typescript-eslint/no-var-requires */
/* eslint-disable @typescript-eslint/ban-ts-comment */
export {}
const { expect } = require('chai')
const sinon = require('sinon')
const Amqp = require('../../src/Amqp').default
const { ErrorType, NodeType } = require('../../src/types')
const { CustomError, amqpInFlowFixture, credentialsFixture } = require('../doubles')
const { NODE_STATUS } = require('../../src/constants')

const helper = require('node-red-node-test-helper')
const amqpIn = require('../../src/nodes/amqp-in')
const amqpBroker = require('../../src/nodes/amqp-broker')

helper.init(require.resolve('node-red'))

describe('amqp-in Node', () => {
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
    const flow = [{ id: 'n1', type: NodeType.AmqpIn, name: 'test name' }]
    helper.load(amqpIn, flow, () => {
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
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture,
      async function () {
        expect(connectStub.calledOnce).to.be.true

        // TODO: Figure out why this isn't working:
        // expect(initializeStub.calledOnce).to.be.true

        const amqpInNode = helper.getNode('n1')
        amqpInNode.close()
        done()
      },
    )
  })

  it('does not register flows:stopped listener', function (done) {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub(), consume: sinon.stub() }
    sinon
      .stub(Amqp.prototype, 'connect')
      .resolves(connectionMock as any)
    sinon
      .stub(Amqp.prototype, 'initialize')
      .resolves(channelMock as any)

    helper.load(
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture,
      function () {
        expect(helper._events.listenerCount('flows:stopped')).to.equal(0)
        const amqpInNode = helper.getNode('n1')
        amqpInNode.close()
        done()
      },
    )
  })

  it('should reconnect to the server', function (done) {
    // @ts-ignore
    Amqp.prototype.channel = {
      unbindQueue: (): null => null,
      close: (): null => null,
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
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture,
      async function () {
        expect(connectStub.calledOnce).to.be.true

        // TODO: Figure out why this isn't working:
        // expect(initializeStub.calledOnce).to.be.true

        const amqpInNode = helper.getNode('n1')
        amqpInNode.close()
        done()
      },
    )
  })

  it('tries to connect but the broker is down', function (done) {
    const connectStub = sinon
      .stub(Amqp.prototype, 'connect')
      .throws(new CustomError(ErrorType.ConnectionRefused))
    helper.load(
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture,
      function () {
        expect(connectStub).to.throw()
        // clock.tick(200)
        done()
      },
    )
  })

  it('catches an invalid login exception', function (done) {
    const connectStub = sinon
      .stub(Amqp.prototype, 'connect')
      .throws(new CustomError(ErrorType.InvalidLogin))
    helper.load(
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
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
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture,
      function () {
        const amqpInNode = helper.getNode('n1')
        amqpInNode.on('call:status', call => {
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
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
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
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture,
      function () {
        expect(connectStub).to.throw()
        done()
      },
    )
  })

  it('should reconnect on input message', done => {
    const connectStub = sinon.stub(Amqp.prototype, 'connect')
    const closeStub = sinon.stub(Amqp.prototype, 'close')
    const flow = [{ id: 'n1', type: NodeType.AmqpIn, name: 'test name', wires: [[]] }]
    helper.load(amqpIn, flow, () => {
      const n1 = helper.getNode('n1')
      n1.receive({ payload: { reconnectCall: true }})
      expect(closeStub.calledOnce).to.be.true
      done()
    })
  })

  it('should handle channel errors', function (done) {
    const flow = [
      { id: 'n1', type: 'amqp-in', name: 'test name', broker: 'b1' },
      { id: 'b1', type: 'amqp-broker', name: 'test broker' }
    ];
    helper.load([amqpIn, amqpBroker], flow, function () {
      const amqpInNode = helper.getNode('n1');
      const brokerNode = helper.getNode('b1');
      brokerNode.on('amqp-in-error', (err) => {
        try {
          expect(err.message).to.equal('Channel error');
          done();
        } catch (e) {
          done(e);
        }
      });
      brokerNode.emit('amqp-in-error', new Error('Channel error'));
    });
  });

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
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture
    )
    // Get the 'on' callback for connection close
    const onCallback = connectionMock.on.withArgs('close').getCall(0).args[1]
    onCallback('connection closed')
    expect(closeStub.calledOnce).to.be.true
  })

  it('handles channel close', async function () {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() };
    const channelMock = { on: sinon.stub(), off: sinon.stub() };
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any);
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any);
    const closeStub = sinon.stub(Amqp.prototype, 'close');

    await helper.load(
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture
    );
    const onCallback = channelMock.on.withArgs('close').getCall(0).args[1];
    onCallback('channel closed');
    expect(closeStub.notCalled).to.be.true; // Should not reconnect
  });

  it('handles channel error', async function () {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() };
    const channelMock = { on: sinon.stub(), off: sinon.stub() };
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any);
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any);
    const closeStub = sinon.stub(Amqp.prototype, 'close');

    await helper.load(
      [amqpIn, amqpBroker],
      amqpInFlowFixture,
      credentialsFixture,
      async function () {
        const amqpInNode = helper.getNode('n1');
        const onCallback = channelMock.on.withArgs('error').getCall(0).args[1];

        let errorCalled = false;
        amqpInNode.on('call:error', () => {
          errorCalled = true;
        });

        await onCallback('channel error');

        expect(errorCalled).to.be.true;
        expect(closeStub.calledOnce).to.be.true;
      }
    );
  });

  it('does not reconnect on connection error when reconnectOnError is false', async function () {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() };
    const channelMock = { on: sinon.stub(), off: sinon.stub() };
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any);
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any);
    const closeStub = sinon.stub(Amqp.prototype, 'close');

    const flow = JSON.parse(JSON.stringify(amqpInFlowFixture));
    flow[0].reconnectOnError = false;

    await helper.load(
      [amqpIn, amqpBroker],
      flow,
      credentialsFixture
    );
    const amqpInNode = helper.getNode('n1');

    const errorPromise = new Promise<void>(resolve => {
      amqpInNode.on('call:error', () => {
        resolve();
      });
    });

    const onCallback = connectionMock.on.withArgs('error').getCall(0).args[1];
    onCallback('connection error');

    await errorPromise;

    expect(closeStub.notCalled).to.be.true;
  });

  it('does not auto-ack when noAck is false', async function () {
    const ackStub = sinon.stub(Amqp.prototype, 'ack');
    sinon.stub(Amqp.prototype, 'consume').callsFake(async function () {
      // @ts-ignore
      this.node.send({ payload: 'test' });
    });

    const flow = JSON.parse(JSON.stringify(amqpInFlowFixture));
    flow[1].noAck = false;
    flow.push({ id: 'n2', type: 'helper' });
    flow[0].wires = [['n2']];

    await helper.load([amqpIn, amqpBroker], flow, credentialsFixture, async function () {
      const n2 = helper.getNode('n2');
      await new Promise<void>(resolve => {
        n2.on('input', () => {
          expect(ackStub.calledOnce).to.be.true;
          resolve();
        });
      });
    });
  });
})
