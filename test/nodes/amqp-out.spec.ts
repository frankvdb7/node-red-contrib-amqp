/* eslint-disable @typescript-eslint/no-var-requires */
/* eslint-disable @typescript-eslint/ban-ts-comment */
export {}
const { expect } = require('chai')
const sinon = require('sinon')
const Amqp = require('../../src/Amqp').default
const { ErrorType, NodeType } = require('../../src/types')
const { CustomError, amqpOutFlowFixture, credentialsFixture } = require('../doubles')
const { NODE_STATUS } = require('../../src/constants')

const helper = require('node-red-node-test-helper')
const amqpOut = require('../../src/nodes/amqp-out')
const amqpBroker = require('../../src/nodes/amqp-broker')

helper.init(require.resolve('node-red'))

describe('amqp-out Node', () => {
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
    const flow = [{ id: 'n1', type: NodeType.AmqpOut, name: 'test name' }]
    helper.load(amqpOut, flow, () => {
      const n1 = helper.getNode('n1')
      n1.should.have.property('name', 'test name')
      done()
    })
  })

  it('should connect to the server and send some messages', function (done) {
    // @ts-ignore
    Amqp.prototype.channel = {
      unbindQueue: (): null => null,
      close: (): null => null,
      off: (): null => null,
      on: (): null => null,
    }
    // @ts-ignore
    Amqp.prototype.connection = {
      close: (): null => null,
      off: (): null => null,
      on: (): null => null,
    }
    const connectStub = sinon
      .stub(Amqp.prototype, 'connect')
      .resolves(Amqp.prototype.connection as any)
    sinon.stub(Amqp.prototype, 'initialize').resolves(Amqp.prototype.channel as any)

    helper.load(
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
      credentialsFixture,
      async function () {
        expect(connectStub.calledOnce).to.be.true
        // TODO: Figure out why this isn't working:
        // expect(initializeStub.calledOnce).to.be.true

        const amqpOutNode = helper.getNode('n1')
        amqpOutNode.receive({ payload: 'foo', routingKey: 'bar' })
        amqpOutNode.receive({ payload: 'foo' })
        amqpOutNode.close()

        done()
      },
    )
  })

  it('should handle JSONata expression evaluation error', function (done) {
    const setRoutingKeyStub = sinon.stub(Amqp.prototype, 'setRoutingKey');
    const publishStub = sinon.stub(Amqp.prototype, 'publish');

    const flowFixture = JSON.parse(JSON.stringify(amqpOutFlowFixture));
    flowFixture[0].exchangeRoutingKeyType = 'jsonata';
    flowFixture[0].exchangeRoutingKey = 'a.b.c'; // An expression that will fail on a simple payload

    helper.load([amqpOut, amqpBroker], flowFixture, credentialsFixture, function () {
      const amqpOutNode = helper.getNode('n1');

      amqpOutNode.on('call:error', (call) => {
        try {
          expect(call.args[0]).to.match(/Failed to evaluate JSONata expression/);
          expect(setRoutingKeyStub.notCalled).to.be.true;
          expect(publishStub.notCalled).to.be.true;
          done();
        } catch (e) {
          done(e);
        }
      });

      amqpOutNode.receive({ payload: 'foo' });
    });
  });

  it('should handle dynamic routing key from `flow` context', function (done) {
    const setRoutingKeyStub = sinon.stub(Amqp.prototype, 'setRoutingKey');
    const publishStub = sinon.stub(Amqp.prototype, 'publish');

    const flowFixture = [...amqpOutFlowFixture];
    flowFixture[0].exchangeRoutingKeyType = 'flow';
    flowFixture[0].exchangeRoutingKey = 'myFlowVar';

    helper.load([amqpOut, amqpBroker], flowFixture, credentialsFixture, function () {
      const amqpOutNode = helper.getNode('n1');
      amqpOutNode.context().flow.set('myFlowVar', 'flow_routing_key');
      amqpOutNode.receive({ payload: 'foo' });

      setTimeout(() => {
        try {
          expect(setRoutingKeyStub.calledWith('flow_routing_key')).to.be.true;
          done();
        } catch (e) {
          done(e);
        }
      }, 50);
    });
  });

  it('should handle dynamic routing key from `global` context', function (done) {
    const setRoutingKeyStub = sinon.stub(Amqp.prototype, 'setRoutingKey');
    const publishStub = sinon.stub(Amqp.prototype, 'publish');

    const flowFixture = [...amqpOutFlowFixture];
    flowFixture[0].exchangeRoutingKeyType = 'global';
    flowFixture[0].exchangeRoutingKey = 'myGlobalVar';

    helper.load([amqpOut, amqpBroker], flowFixture, credentialsFixture, function () {
      const amqpOutNode = helper.getNode('n1');
      amqpOutNode.context().global.set('myGlobalVar', 'global_routing_key');
      amqpOutNode.receive({ payload: 'foo' });

      setTimeout(() => {
        expect(setRoutingKeyStub.calledWith('global_routing_key')).to.be.true;
        done();
      }, 50);
    });
  });

  it('should not stringify payload when doNotStringifyPayload header is set', function (done) {
    const payload = { data: 'test' };
    const publishStub = sinon.stub(Amqp.prototype, 'publish');
    const flowFixture = [...amqpOutFlowFixture];

    helper.load([amqpOut, amqpBroker], flowFixture, credentialsFixture, function () {
      const amqpOutNode = helper.getNode('n1');
      amqpOutNode.receive({
        payload,
        properties: { headers: { doNotStringifyPayload: true } },
      });

      setTimeout(() => {
        expect(publishStub.calledOnceWith(payload, sinon.match.any)).to.be.true;
        done();
      }, 50);
    });
  });

  it('handles error when switching vhost', function (done) {
    const setVhostStub = sinon.stub(Amqp.prototype, 'setVhost').rejects(new Error('vhost switch failed'));

    helper.load([amqpOut, amqpBroker], amqpOutFlowFixture, credentialsFixture, function () {
      const amqpOutNode = helper.getNode('n1');

      amqpOutNode.on('call:error', (call) => {
        try {
          expect(setVhostStub.calledWith('vh2')).to.be.true;
          done();
        } catch (e) {
          done(e);
        }
      });

      amqpOutNode.receive({ payload: 'foo', vhost: 'vh2' });
    });
  });

  it('does not register flows:stopped listener', function (done) {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub() }
    sinon
      .stub(Amqp.prototype, 'connect')
      .resolves(connectionMock as any)
    sinon
      .stub(Amqp.prototype, 'initialize')
      .resolves(channelMock as any)

    helper.load(
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
      credentialsFixture,
      function () {
        expect(helper._events.listenerCount('flows:stopped')).to.equal(0)
        const n1 = helper.getNode('n1')
        n1.close()
        done()
      },
    )
  })

  it('should connect to the server and send some messages with a dynamic routing key from `msg`', function (done) {
    // @ts-ignore
    Amqp.prototype.channel = {
      unbindQueue: (): null => null,
      close: (): null => null,
      off: (): null => null,
      on: (): null => null,
    }
    // @ts-ignore
    Amqp.prototype.connection = {
      close: (): null => null,
      off: (): null => null,
      on: (): null => null,
    }
    const connectStub = sinon
      .stub(Amqp.prototype, 'connect')
      .resolves(Amqp.prototype.connection as any)
    sinon.stub(Amqp.prototype, 'initialize').resolves(Amqp.prototype.channel as any)

    const flowFixture = [...amqpOutFlowFixture]
    // @ts-ignore
    flowFixture[0].exchangeRoutingKeyType = 'msg'

    helper.load(
      [amqpOut, amqpBroker],
      flowFixture,
      credentialsFixture,
      async function () {
        expect(connectStub.calledOnce).to.be.true
        // TODO: Figure out why this isn't working:
        // expect(initializeStub.calledOnce).to.be.true

        const amqpOutNode = helper.getNode('n1')
        amqpOutNode.receive({ payload: 'foo' })
        amqpOutNode.close()

        done()
      },
    )
  })

  it('should connect to the server and send some messages with a dynamic jsonata routing key', function (done) {
    // @ts-ignore
    Amqp.prototype.channel = {
      unbindQueue: (): null => null,
      close: (): null => null,
      off: (): null => null,
      on: (): null => null,
    }
    // @ts-ignore
    Amqp.prototype.connection = {
      close: (): null => null,
      off: (): null => null,
      on: (): null => null,
    }
    const connectStub = sinon
      .stub(Amqp.prototype, 'connect')
      .resolves(Amqp.prototype.connection as any)
    sinon.stub(Amqp.prototype, 'initialize').resolves(Amqp.prototype.channel as any)

    const flowFixture = [...amqpOutFlowFixture]
    // @ts-ignore
    flowFixture[0].exchangeRoutingKeyType = 'jsonata'

    helper.load(
      [amqpOut, amqpBroker],
      flowFixture,
      credentialsFixture,
      async function () {
        expect(connectStub.calledOnce).to.be.true
        // TODO: Figure out why this isn't working:
        // expect(initializeStub.calledOnce).to.be.true

        const amqpOutNode = helper.getNode('n1')
        amqpOutNode.receive({ payload: 'foo' })
        amqpOutNode.close()

        done()
      },
    )
  })

  it('switches vhost dynamically from msg', function (done) {
    // @ts-ignore
    Amqp.prototype.channel = {
      unbindQueue: (): null => null,
      close: (): null => null,
      off: (): null => null,
      on: (): null => null,
    }
    // @ts-ignore
    Amqp.prototype.connection = {
      close: (): null => null,
      off: (): null => null,
      on: (): null => null,
    }
    sinon.stub(Amqp.prototype, 'connect').resolves(Amqp.prototype.connection as any)
    sinon.stub(Amqp.prototype, 'initialize').resolves(Amqp.prototype.channel as any)
    const setVhostStub = sinon.stub(Amqp.prototype, 'setVhost').resolves()

    helper.load(
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
      credentialsFixture,
      function () {
        const amqpOutNode = helper.getNode('n1')
        amqpOutNode.receive({ payload: 'foo', vhost: 'vh2' })
        setTimeout(() => {
          expect(setVhostStub.calledWith('vh2')).to.be.true
          amqpOutNode.close()
          done()
        }, 0)
      },
    )
  })

  it('removes listeners before switching vhost', function (done) {
    const connectionMock = {
      off: sinon.spy(),
      close: (): null => null,
    }
    const channelMock = {
      off: sinon.spy(),
      unbindQueue: (): null => null,
      close: (): null => null,
    }

    sinon
      .stub(Amqp.prototype, 'connect')
      .callsFake(async function () {
        // @ts-ignore
        this.connection = connectionMock
        return connectionMock as any
      })

    sinon
      .stub(Amqp.prototype, 'initialize')
      .callsFake(async function () {
        // @ts-ignore
        this.channel = channelMock
        return channelMock as any
      })

    const setVhostStub = sinon.stub(Amqp.prototype, 'setVhost').resolves()

    helper.load(
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
      credentialsFixture,
      function () {
        const amqpOutNode = helper.getNode('n1')
        amqpOutNode.receive({ payload: 'foo', vhost: 'vh2' })
        setTimeout(() => {
          sinon.assert.callOrder(connectionMock.off as any, setVhostStub)
          sinon.assert.callOrder(channelMock.off as any, setVhostStub)
          amqpOutNode.close()
          done()
        }, 0)
      },
    )
  })

  it('tries to connect but the broker is down', function (done) {
    const connectStub = sinon
      .stub(Amqp.prototype, 'connect')
      .throws(new CustomError(ErrorType.ConnectionRefused))
    helper.load(
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
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
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
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
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
      credentialsFixture,
      function () {
        const amqpOutNode = helper.getNode('n1')
        amqpOutNode.on('call:status', call => {
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
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
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
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
      credentialsFixture,
      function () {
        expect(connectStub).to.throw()
        done()
      },
    )
  })

  it('handles connection close', function (done) {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub() }
    sinon
      .stub(Amqp.prototype, 'connect')
      .resolves(connectionMock as any)
    sinon
      .stub(Amqp.prototype, 'initialize')
      .resolves(channelMock as any)
    const closeStub = sinon.stub(Amqp.prototype, 'close')

    helper.load(
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
      credentialsFixture,
      function () {
        // Get the 'on' callback for connection close
        const onCallback = connectionMock.on.withArgs('close').getCall(0).args[1]
        onCallback('connection closed')
        expect(closeStub.calledOnce).to.be.true
        done()
      },
    )
  })

  it('should handle connection errors', function (done) {
    const flow = [
      { id: 'n1', type: 'amqp-out', name: 'test name', broker: 'b1' },
      { id: 'b1', type: 'amqp-broker', name: 'test broker' }
    ];
    helper.load([amqpOut, amqpBroker], flow, function () {
      const amqpOutNode = helper.getNode('n1');
      const brokerNode = helper.getNode('b1');
      brokerNode.on('amqp-out-error', (err) => {
        try {
          expect(err.message).to.equal('Connection error');
          done();
        } catch (e) {
          done(e);
        }
      });
      brokerNode.emit('amqp-out-error', new Error('Connection error'));
    });
  });

  it('does not reconnect on connection error when reconnectOnError is false', function (done) {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() };
    const channelMock = { on: sinon.stub(), off: sinon.stub() };
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any);
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any);
    const closeStub = sinon.stub(Amqp.prototype, 'close');

    const flow = JSON.parse(JSON.stringify(amqpOutFlowFixture));
    flow[1].reconnectOnError = false;

    helper.load([amqpOut, amqpBroker], flow, credentialsFixture, function () {
      const onCallback = connectionMock.on.withArgs('error').getCall(0).args[1];
      onCallback('connection error');
      expect(closeStub.called).to.be.false;
      done();
    });
  });

  it('reconnects on initialization failure', async function () {
    const clock = sinon.useFakeTimers();
    const connectStub = sinon.stub(Amqp.prototype, 'connect')
      .onFirstCall().rejects(new Error('Initial connection failed'))
      .onSecondCall().resolves({ on: sinon.stub(), off: sinon.stub(), close: sinon.stub() } as any);

    helper.load([amqpOut, amqpBroker], amqpOutFlowFixture, credentialsFixture, async function () {
      await clock.tickAsync(2500); // Wait for the reconnect timeout
      expect(connectStub.callCount).to.equal(2);
      clock.restore();
    });
  });

  it('handles invalid amqpProperties gracefully', function (done) {
    const publishStub = sinon.stub(Amqp.prototype, 'publish');
    const flowFixture = JSON.parse(JSON.stringify(amqpOutFlowFixture));
    flowFixture[0].amqpProperties = '{"invalid-json"'; // Malformed JSON

    helper.load([amqpOut, amqpBroker], flowFixture, credentialsFixture, function () {
      const amqpOutNode = helper.getNode('n1');
      const msgProperties = { contentType: 'application/json' };
      amqpOutNode.receive({ payload: 'test', properties: msgProperties });

      setTimeout(() => {
        expect(publishStub.calledOnceWith(sinon.match.any, msgProperties)).to.be.true;
        done();
      }, 50);
    });
  });
})
