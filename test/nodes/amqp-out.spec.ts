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

  it('should handle JSONata expression evaluation error', async function () {
    const setRoutingKeyStub = sinon.stub(Amqp.prototype, 'setRoutingKey');
    const publishStub = sinon.stub(Amqp.prototype, 'publish');
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub() }
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)

    const flowFixture = JSON.parse(JSON.stringify(amqpOutFlowFixture));
    flowFixture[0].exchangeRoutingKeyType = 'jsonata';
    flowFixture[0].exchangeRoutingKey = '$foo()'; // An invalid expression

    await helper.load([amqpOut, amqpBroker], flowFixture, credentialsFixture);
    const amqpOutNode = helper.getNode('n1');
    const callErrors: unknown[] = [];
    amqpOutNode.on('call:error', (call) => {
      callErrors.push(call.args[0]);
    });

    await amqpOutNode.receive({ payload: 'foo' });
    await new Promise(resolve => setTimeout(resolve, 50));

    const hasJsonataError = callErrors.some(arg => {
      if (arg instanceof Error) {
        return /JSONata/i.test(arg.message);
      }
      return /Failed to evaluate JSONata expression/i.test(String(arg));
    });

    expect(hasJsonataError).to.be.true;
    expect(setRoutingKeyStub.notCalled).to.be.true;
    expect(publishStub.notCalled).to.be.true;
  });

  it('calls done with error when JSONata expression evaluation fails', async function () {
    const publishStub = sinon.stub(Amqp.prototype, 'publish');
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub() }
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)

    const flowFixture = JSON.parse(JSON.stringify(amqpOutFlowFixture));
    flowFixture[0].exchangeRoutingKeyType = 'jsonata';
    flowFixture[0].exchangeRoutingKey = '$foo()';

    await helper.load([amqpOut, amqpBroker], flowFixture, credentialsFixture);
    const amqpOutNode = helper.getNode('n1');
    const callErrors: unknown[] = [];
    amqpOutNode.on('call:error', call => {
      callErrors.push(call.args[0]);
    });

    await amqpOutNode.receive({ payload: 'foo' });
    await new Promise(resolve => setTimeout(resolve, 50));

    const doneError = callErrors.find(arg => arg instanceof Error) as Error | undefined;
    expect(doneError).to.not.equal(undefined);
    expect(publishStub.notCalled).to.be.true;
  });

  it('should handle dynamic routing key from `flow` context', async function () {
    const setRoutingKeyStub = sinon.stub(Amqp.prototype, 'setRoutingKey');
    sinon.stub(Amqp.prototype, 'publish');
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub() }
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)

    const flowFixture = JSON.parse(JSON.stringify(amqpOutFlowFixture));
    flowFixture[0].exchangeRoutingKeyType = 'flow';
    flowFixture[0].exchangeRoutingKey = 'myFlowVar';
    flowFixture[0].z = 'f1';
    flowFixture[1].z = 'f1';
    flowFixture.push({ id: 'f1', type: 'tab', label: 'test' });

    await helper.load([amqpOut, amqpBroker], flowFixture, credentialsFixture);
    const amqpOutNode = helper.getNode('n1');
    const flowContext = amqpOutNode.context().flow;
    flowContext.set('myFlowVar', 'flow_routing_key');
    await amqpOutNode.receive({ payload: 'foo' });

    expect(setRoutingKeyStub.calledWith('flow_routing_key')).to.be.true;
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

  it('calls done with error when routing key evaluation path throws', async function () {
    const setRoutingKeyStub = sinon
      .stub(Amqp.prototype, 'setRoutingKey')
      .throws(new Error('routing key evaluation failed'));
    const publishStub = sinon.stub(Amqp.prototype, 'publish');
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() };
    const channelMock = { on: sinon.stub(), off: sinon.stub() };
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any);
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any);

    const flowFixture = JSON.parse(JSON.stringify(amqpOutFlowFixture));
    flowFixture[0].exchangeRoutingKeyType = 'msg';
    flowFixture[0].exchangeRoutingKey = 'routingKey';

    await helper.load([amqpOut, amqpBroker], flowFixture, credentialsFixture);
    const amqpOutNode = helper.getNode('n1');
    const callErrors: unknown[] = [];
    amqpOutNode.on('call:error', call => {
      callErrors.push(call.args[0]);
    });

    await amqpOutNode.receive({ payload: 'foo', routingKey: 'test.route' });
    await new Promise(resolve => setTimeout(resolve, 50));

    const doneError = callErrors.find(arg => arg instanceof Error) as Error | undefined;
    expect(doneError).to.not.equal(undefined);
    expect(doneError?.message).to.match(/routing key evaluation failed/i);
    expect(setRoutingKeyStub.calledOnce).to.be.true;
    expect(publishStub.notCalled).to.be.true;
  });

  it('passes object payload to publish without pre-serializing', function (done) {
    const payload = { data: 'test' }
    const publishStub = sinon.stub(Amqp.prototype, 'publish')
    const flowFixture = [...amqpOutFlowFixture]

    helper.load([amqpOut, amqpBroker], flowFixture, credentialsFixture, function () {
      const amqpOutNode = helper.getNode('n1')
      amqpOutNode.receive({ payload })

      setTimeout(() => {
        expect(publishStub.calledOnceWith(payload, sinon.match.any)).to.be.true
        done()
      }, 50)
    })
  })

  it('passes string payload to publish without JSON stringifying', function (done) {
    const payload = 'hello'
    const publishStub = sinon.stub(Amqp.prototype, 'publish')
    const flowFixture = [...amqpOutFlowFixture]

    helper.load([amqpOut, amqpBroker], flowFixture, credentialsFixture, function () {
      const amqpOutNode = helper.getNode('n1')
      amqpOutNode.receive({ payload })

      setTimeout(() => {
        expect(publishStub.calledOnceWith(payload, sinon.match.any)).to.be.true
        done()
      }, 50)
    })
  })

  it('handles error when switching vhost', async function () {
    const setVhostStub = sinon.stub(Amqp.prototype, 'setVhost').rejects(new Error('vhost switch failed'));

    await helper.load([amqpOut, amqpBroker], amqpOutFlowFixture, credentialsFixture, async function () {
      const amqpOutNode = helper.getNode('n1');

      let errorCalled = false;
      amqpOutNode.on('call:error', () => {
        errorCalled = true;
      });

      await amqpOutNode.receive({ payload: 'foo', vhost: 'vh2' });

      expect(setVhostStub.calledWith('vh2')).to.be.true;
      expect(errorCalled).to.be.true;
    });
  });

  it('calls done with error and does not publish when vhost switch fails', async function () {
    const setVhostStub = sinon.stub(Amqp.prototype, 'setVhost').rejects(new Error('vhost switch failed'));
    const publishStub = sinon.stub(Amqp.prototype, 'publish');
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub() }
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)

    await helper.load([amqpOut, amqpBroker], amqpOutFlowFixture, credentialsFixture);
    const amqpOutNode = helper.getNode('n1');
    const callErrors: unknown[] = [];
    amqpOutNode.on('call:error', call => {
      callErrors.push(call.args[0]);
    });

    await amqpOutNode.receive({ payload: 'foo', vhost: 'vh2' });
    await new Promise(resolve => setTimeout(resolve, 50));

    expect(setVhostStub.calledOnceWith('vh2')).to.be.true;
    const doneError = callErrors.find(arg => arg instanceof Error) as Error | undefined;
    expect(doneError).to.not.equal(undefined);
    expect(doneError?.message).to.match(/vhost switch failed/i);
    expect(publishStub.notCalled).to.be.true;
  });

  it('calls done with error when publish fails', async function () {
    const publishStub = sinon.stub(Amqp.prototype, 'publish').rejects(new Error('publish failed'));
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub() }
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)

    await helper.load([amqpOut, amqpBroker], amqpOutFlowFixture, credentialsFixture);
    const amqpOutNode = helper.getNode('n1');
    const callErrors: unknown[] = [];
    amqpOutNode.on('call:error', call => {
      callErrors.push(call.args[0]);
    });

    await amqpOutNode.receive({ payload: 'foo' });
    await new Promise(resolve => setTimeout(resolve, 50));

    expect(publishStub.calledOnce).to.be.true;
    const doneError = callErrors.find(arg => arg instanceof Error) as Error | undefined;
    expect(doneError).to.not.equal(undefined);
    expect(doneError?.message).to.match(/publish failed/i);
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

  it('completes node close when amqp.close fails', async function () {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub() }
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)
    const closeStub = sinon.stub(Amqp.prototype, 'close').rejects(new Error('close failed'))

    await helper.load(
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
      credentialsFixture,
    )
    const amqpOutNode = helper.getNode('n1')
    await amqpOutNode.close()

    expect(closeStub.calledOnce).to.be.true
  })

  it('closes amqp when initialization fails after connect', async function () {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
    sinon.stub(Amqp.prototype, 'initialize').rejects(new Error('init failed'))
    const closeStub = sinon.stub(Amqp.prototype, 'close').resolves()

    await helper.load(
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
      credentialsFixture,
    )

    expect(closeStub.called).to.be.true
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

  it('handles non-object connect errors', function (done) {
    const connectStub = sinon.stub(Amqp.prototype, 'connect').throws('boom')
    helper.load(
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
      credentialsFixture,
      function () {
        const amqpOutNode = helper.getNode('n1')
        amqpOutNode.on('call:status', call => {
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

  it('reconnects on connection close without error argument', function (done) {
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
        const onCallback = connectionMock.on.withArgs('close').getCall(0).args[1]
        onCallback()
        expect(closeStub.calledOnce).to.be.true
        done()
      },
    )
  })

  it('does not reconnect after node close when late connection close event arrives', async function () {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub() }
    sinon
      .stub(Amqp.prototype, 'connect')
      .resolves(connectionMock as any)
    sinon
      .stub(Amqp.prototype, 'initialize')
      .resolves(channelMock as any)
    const closeStub = sinon.stub(Amqp.prototype, 'close').resolves()

    await helper.load(
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
      credentialsFixture,
    )
    const n1 = helper.getNode('n1')
    await n1.close()

    const onCallback = connectionMock.on.withArgs('close').getCall(0).args[1]
    await onCallback('late connection close')

    expect(closeStub.calledOnce).to.be.true
  })

  it('coalesces duplicate reconnect triggers into a single reconnect cycle', async function () {
    const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true })
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub() }
    const connectStub = sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)
    const closeStub = sinon.stub(Amqp.prototype, 'close').resolves()

    await helper.load(
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
      credentialsFixture,
    )

    const onConnClose = connectionMock.on.withArgs('close').getCall(0).args[1]
    const onChannelClose = channelMock.on.withArgs('close').getCall(0).args[1]

    await onConnClose()
    await onChannelClose()
    expect(closeStub.calledOnce).to.be.true

    await clock.tickAsync(2001)
    expect(connectStub.callCount).to.equal(2)
    clock.restore()
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
})
