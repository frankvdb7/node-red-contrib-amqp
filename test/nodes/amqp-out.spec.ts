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

  it('calls done with error when JSONata expression is malformed', async function () {
    const publishStub = sinon.stub(Amqp.prototype, 'publish');
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() };
    const channelMock = { on: sinon.stub(), off: sinon.stub() };
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any);
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any);

    const flowFixture = JSON.parse(JSON.stringify(amqpOutFlowFixture));
    flowFixture[0].exchangeRoutingKeyType = 'jsonata';
    flowFixture[0].exchangeRoutingKey = '(';

    await helper.load([amqpOut, amqpBroker], flowFixture, credentialsFixture);
    const amqpOutNode = helper.getNode('n1');
    const callErrors: unknown[] = [];
    amqpOutNode.on('call:error', call => {
      callErrors.push(call.args[0]);
    });

    await amqpOutNode.receive({ payload: 'foo' });
    await new Promise(resolve => setTimeout(resolve, 50));

    const doneError = callErrors.find(arg => arg instanceof Error) as Error | undefined;
    const hasLoggedJsonataError = callErrors.some(arg =>
      /Failed to evaluate JSONata expression/i.test(String(arg)),
    );
    expect(doneError).to.not.equal(undefined);
    expect(hasLoggedJsonataError).to.be.true;
    expect(publishStub.notCalled).to.be.true;
  });

  it('should handle dynamic routing key from `flow` context', async function () {
    const publishStub = sinon.stub(Amqp.prototype, 'publish');
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

    expect(publishStub.calledWith(
      'foo',
      sinon.match.any,
      'flow_routing_key',
    )).to.be.true;
  });

  it('should handle dynamic routing key from `global` context', function (done) {
    const publishStub = sinon.stub(Amqp.prototype, 'publish');

    const flowFixture = JSON.parse(JSON.stringify(amqpOutFlowFixture));
    flowFixture[0].exchangeRoutingKeyType = 'global';
    flowFixture[0].exchangeRoutingKey = 'myGlobalVar';

    helper.load([amqpOut, amqpBroker], flowFixture, credentialsFixture, function () {
      const amqpOutNode = helper.getNode('n1');
      amqpOutNode.context().global.set('myGlobalVar', 'global_routing_key');
      amqpOutNode.receive({ payload: 'foo' });

      setTimeout(() => {
        expect(publishStub.calledWith(
          'foo',
          sinon.match.any,
          'global_routing_key',
        )).to.be.true;
        done();
      }, 50);
    });
  });

  it('calls done with error when routing key evaluation path throws', async function () {
    const publishStub = sinon.stub(Amqp.prototype, 'publish');
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() };
    const channelMock = { on: sinon.stub(), off: sinon.stub() };
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any);
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any);

    const flowFixture = JSON.parse(JSON.stringify(amqpOutFlowFixture));
    flowFixture[0].exchangeRoutingKeyType = 'msg';
    flowFixture[0].exchangeRoutingKey = 'routingKey';

    await helper.load([amqpOut, amqpBroker], flowFixture, credentialsFixture);
    sinon
      .stub(helper._RED.util, 'evaluateNodeProperty')
      .throws(new Error('routing key evaluation failed'));
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
    expect(publishStub.notCalled).to.be.true;
  });

  it('passes object payload to publish without pre-serializing', function (done) {
    const payload = { data: 'test' }
    const publishStub = sinon.stub(Amqp.prototype, 'publish')
    const flowFixture = JSON.parse(JSON.stringify(amqpOutFlowFixture))

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
    const flowFixture = JSON.parse(JSON.stringify(amqpOutFlowFixture))

    helper.load([amqpOut, amqpBroker], flowFixture, credentialsFixture, function () {
      const amqpOutNode = helper.getNode('n1')
      amqpOutNode.receive({ payload })

      setTimeout(() => {
        expect(publishStub.calledOnceWith(payload, sinon.match.any)).to.be.true
        done()
      }, 50)
    })
  })

  it('publishes ordinary messages concurrently', async function () {
    const connectionMock = {
      on: sinon.stub(),
      off: sinon.stub(),
      close: sinon.stub(),
    }
    const channelMock = {
      on: sinon.stub(),
      off: sinon.stub(),
    }
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)

    let releaseFirstPublish: () => void = () => undefined
    const firstPublishPending = new Promise<void>(resolve => {
      releaseFirstPublish = resolve
    })
    const publishStub = sinon
      .stub(Amqp.prototype, 'publish')
      .callsFake((payload: unknown) =>
        payload === 'first' ? firstPublishPending : Promise.resolve(),
      )

    await helper.load(
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
      credentialsFixture,
    )
    const node = helper.getNode('n1')

    node.receive({ payload: 'first' })
    node.receive({ payload: 'second' })
    await new Promise(resolve => setTimeout(resolve, 20))
    const callsBeforeFirstConfirm = publishStub.callCount

    releaseFirstPublish()
    await new Promise(resolve => setTimeout(resolve, 0))

    expect(callsBeforeFirstConfirm).to.equal(2)
  })

  it('publishes concurrent routing-key overrides as immutable arguments', async function () {
    const connectionMock = {
      on: sinon.stub(),
      off: sinon.stub(),
      close: sinon.stub(),
    }
    const channelMock = {
      on: sinon.stub(),
      off: sinon.stub(),
    }
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)

    let releaseFirstPublish: () => void = () => undefined
    const firstPublishPending = new Promise<void>(resolve => {
      releaseFirstPublish = resolve
    })
    const publishStub = sinon
      .stub(Amqp.prototype, 'publish')
      .callsFake((payload: unknown) =>
        payload === 'first' ? firstPublishPending : Promise.resolve(),
      )

    const flow = JSON.parse(JSON.stringify(amqpOutFlowFixture))
    flow[0].exchangeRoutingKey = 'configured.route'
    flow[0].exchangeRoutingKeyType = 'str'
    await helper.load([amqpOut, amqpBroker], flow, credentialsFixture)
    const node = helper.getNode('n1')

    node.receive({ payload: 'first', routingKey: 'route.first' })
    node.receive({ payload: 'second', routingKey: 'route.second' })
    await new Promise(resolve => setTimeout(resolve, 20))
    const callsBeforeFirstConfirm = publishStub.callCount

    releaseFirstPublish()
    await new Promise(resolve => setTimeout(resolve, 0))

    expect(callsBeforeFirstConfirm).to.equal(2)
    expect(publishStub.getCalls().map(call => call.args[2])).to.deep.equal([
      'route.first',
      'route.second',
    ])
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

  it('does not register listeners or mark connected when node closes during initialization', async function () {
    let resolveInitialize: (value: unknown) => void
    const initializePromise = new Promise(resolve => {
      resolveInitialize = resolve
    })
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub() }
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
    sinon.stub(Amqp.prototype, 'initialize').returns(initializePromise as any)
    sinon.stub(Amqp.prototype, 'close').resolves()
    const markConnectedStub = sinon.stub(Amqp.prototype, 'markConnected')

    await helper.load([amqpOut, amqpBroker], amqpOutFlowFixture, credentialsFixture)
    const amqpOutNode = helper.getNode('n1')
    const closePromise = amqpOutNode.close()
    resolveInitialize!(channelMock)
    await closePromise
    await new Promise(resolve => setTimeout(resolve, 0))

    expect(markConnectedStub.called).to.be.false
    expect(connectionMock.on.called).to.be.false
    expect(channelMock.on.called).to.be.false
  })

  it('does not set error or invalid status when initialization fails during shutdown', async function () {
    let rejectInitialize: (reason?: unknown) => void
    const initializePromise = new Promise((_, reject) => {
      rejectInitialize = reject
    })
    sinon.stub(Amqp.prototype, 'connect').resolves({ on: sinon.stub(), off: sinon.stub(), close: sinon.stub() } as any)
    sinon.stub(Amqp.prototype, 'initialize').returns(initializePromise as any)
    const closeStub = sinon.stub(Amqp.prototype, 'close').resolves()

    await helper.load([amqpOut, amqpBroker], amqpOutFlowFixture, credentialsFixture)
    const amqpOutNode = helper.getNode('n1')
    const statuses: unknown[] = []
    amqpOutNode.on('call:status', call => statuses.push(call.args[0]))

    const closePromise = amqpOutNode.close()
    rejectInitialize!(new Error('init failed during shutdown'))
    await closePromise
    await new Promise(resolve => setTimeout(resolve, 0))

    const hasErrorOrInvalid = statuses.some(status => {
      const text = status && typeof status === 'object' ? (status as { text?: string }).text : ''
      return text === NODE_STATUS.Error.text || text === NODE_STATUS.Invalid.text
    })
    expect(hasErrorOrInvalid).to.be.false
    expect(closeStub.called).to.be.true
  })

  it('closes cleanly before listeners are assigned', async function () {
    sinon.stub(Amqp.prototype, 'connect').resolves({ on: sinon.stub(), off: sinon.stub(), close: sinon.stub() } as any)
    sinon.stub(Amqp.prototype, 'initialize').returns(new Promise(() => undefined) as any)
    const closeStub = sinon.stub(Amqp.prototype, 'close').resolves()

    await helper.load(
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
      credentialsFixture,
    )

    const amqpOutNode = helper.getNode('n1')
    await amqpOutNode.close()

    expect(closeStub.calledOnce).to.be.true
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

    const flowFixture = JSON.parse(JSON.stringify(amqpOutFlowFixture))
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

    const flowFixture = JSON.parse(JSON.stringify(amqpOutFlowFixture))
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

  it('waits for and invalidates startup initialization before switching vhost', async function () {
    let releaseInitialConnect: (connection: unknown) => void = () => undefined
    const initialConnect = new Promise(resolve => {
      releaseInitialConnect = resolve
    })
    const oldConnection = {
      on: sinon.stub(),
      off: sinon.stub(),
      close: sinon.stub(),
    }
    const newConnection = {
      on: sinon.stub(),
      off: sinon.stub(),
      close: sinon.stub(),
    }
    const newChannel = {
      on: sinon.stub(),
      off: sinon.stub(),
      close: sinon.stub(),
    }
    sinon.stub(Amqp.prototype, 'connect').returns(initialConnect as any)
    const initializeStub = sinon.stub(Amqp.prototype, 'initialize').resolves(newChannel as any)
    const closeStub = sinon.stub(Amqp.prototype, 'close').resolves()
    const setVhostStub = sinon.stub(Amqp.prototype, 'setVhost').resolves()
    sinon.stub(Amqp.prototype, 'getConnection').returns(newConnection as any)
    sinon.stub(Amqp.prototype, 'getChannel').returns(newChannel as any)
    sinon.stub(Amqp.prototype, 'markConnected')
    let notifyPublished: () => void = () => undefined
    const published = new Promise<void>(resolve => {
      notifyPublished = resolve
    })
    sinon.stub(Amqp.prototype, 'publish').callsFake(async () => {
      notifyPublished()
    })

    await helper.load(
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
      credentialsFixture,
    )
    const amqpOutNode = helper.getNode('n1')

    amqpOutNode.receive({ payload: 'switched', vhost: 'new-vhost' })
    await Promise.resolve()
    await Promise.resolve()

    expect(setVhostStub.called).to.be.false

    releaseInitialConnect(oldConnection)
    await published

    expect(closeStub.calledOnce).to.be.true
    expect(initializeStub.called).to.be.false
    expect(setVhostStub.calledOnceWith('new-vhost')).to.be.true
    expect(oldConnection.on.called).to.be.false
    expect(newConnection.on.called).to.be.true
  })

  it('keeps routing keys isolated across concurrent vhost switches', async function () {
    const connectionMock = {
      on: sinon.stub(),
      off: sinon.stub(),
      close: sinon.stub(),
    }
    const channelMock = {
      on: sinon.stub(),
      off: sinon.stub(),
    }
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)
    sinon.stub(Amqp.prototype, 'close').resolves()

    let notifyFirstSwitchStarted: () => void = () => undefined
    const firstSwitchStarted = new Promise<void>(resolve => {
      notifyFirstSwitchStarted = resolve
    })
    let releaseFirstSwitch: () => void = () => undefined
    const firstSwitchPending = new Promise<void>(resolve => {
      releaseFirstSwitch = resolve
    })
    sinon
      .stub(Amqp.prototype, 'setVhost')
      .callsFake(async (vhost: string) => {
        if (vhost === 'vh-a') {
          notifyFirstSwitchStarted()
          await firstSwitchPending
        }
      })

    const published: Array<{ payload: unknown; routingKey: string }> = []
    sinon
      .stub(Amqp.prototype, 'publish')
      .callsFake(async function (
        this: any,
        payload: unknown,
        properties: unknown,
        routingKey: string,
      ) {
        published.push({
          payload,
          routingKey,
        })
      })

    const flow = JSON.parse(JSON.stringify(amqpOutFlowFixture))
    flow[0].exchangeRoutingKeyType = 'str'
    await helper.load([amqpOut, amqpBroker], flow, credentialsFixture)

    const amqpOutNode = helper.getNode('n1')
    amqpOutNode.receive({
      payload: 'message-a',
      routingKey: 'route.a',
      vhost: 'vh-a',
    })
    await firstSwitchStarted

    amqpOutNode.receive({
      payload: 'message-b',
      routingKey: 'route.b',
      vhost: 'vh-b',
    })
    await new Promise(resolve => setTimeout(resolve, 20))
    releaseFirstSwitch()
    await new Promise(resolve => setTimeout(resolve, 20))

    expect(published).to.have.length(2)
    expect(
      published.find(item => item.payload === 'message-a')?.routingKey,
    ).to.equal('route.a')
    expect(
      published.find(item => item.payload === 'message-b')?.routingKey,
    ).to.equal('route.b')
  })

  it('keeps the configured routing key unchanged after a message override', async function () {
    const connectionMock = {
      on: sinon.stub(),
      off: sinon.stub(),
      close: sinon.stub(),
    }
    const channelMock = {
      on: sinon.stub(),
      off: sinon.stub(),
    }
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)
    sinon.stub(Amqp.prototype, 'close').resolves()

    const publishedRoutingKeys: string[] = []
    const configuredRoutingKeys: string[] = []
    let notifyPublishedTwice: () => void = () => undefined
    const publishedTwice = new Promise<void>(resolve => {
      notifyPublishedTwice = resolve
    })
    sinon
      .stub(Amqp.prototype, 'publish')
      .callsFake(async function (
        this: any,
        payload: unknown,
        properties: unknown,
        routingKey: string,
      ) {
        publishedRoutingKeys.push(routingKey)
        configuredRoutingKeys.push(this.config.exchange.routingKey)
        if (publishedRoutingKeys.length === 2) {
          notifyPublishedTwice()
        }
      })

    const flow = JSON.parse(JSON.stringify(amqpOutFlowFixture))
    flow[0].exchangeRoutingKey = 'configured.route'
    flow[0].exchangeRoutingKeyType = 'str'
    await helper.load([amqpOut, amqpBroker], flow, credentialsFixture)

    const amqpOutNode = helper.getNode('n1')
    amqpOutNode.receive({
      payload: 'overridden',
      routingKey: 'message.route',
    })
    amqpOutNode.receive({ payload: 'configured' })
    await publishedTwice

    expect(publishedRoutingKeys).to.deep.equal([
      'message.route',
      'configured.route',
    ])
    expect(configuredRoutingKeys).to.deep.equal([
      'configured.route',
      'configured.route',
    ])
  })

  it('does not publish an in-flight vhost switch after the node closes', async function () {
    const connectionMock = {
      on: sinon.stub(),
      off: sinon.stub(),
      close: sinon.stub(),
    }
    const channelMock = {
      on: sinon.stub(),
      off: sinon.stub(),
    }
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)
    sinon.stub(Amqp.prototype, 'close').resolves()
    sinon.stub(Amqp.prototype, 'getConnection').returns(connectionMock as any)
    sinon.stub(Amqp.prototype, 'getChannel').returns(channelMock as any)

    let notifySwitchStarted: () => void = () => undefined
    const switchStarted = new Promise<void>(resolve => {
      notifySwitchStarted = resolve
    })
    let releaseSwitch: () => void = () => undefined
    const switchPending = new Promise<void>(resolve => {
      releaseSwitch = resolve
    })
    sinon.stub(Amqp.prototype, 'setVhost').callsFake(async () => {
      notifySwitchStarted()
      await switchPending
    })
    const publishStub = sinon.stub(Amqp.prototype, 'publish').resolves()

    await helper.load(
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
      credentialsFixture,
    )

    const amqpOutNode = helper.getNode('n1')
    amqpOutNode.receive({ payload: 'must-not-publish', vhost: 'vh2' })
    await switchStarted
    await amqpOutNode.close()

    releaseSwitch()
    await new Promise(resolve => setTimeout(resolve, 0))

    expect(publishStub.called).to.be.false
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

  it('allows repeated reconnect attempts after close failure', async function () {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub() }
    sinon
      .stub(Amqp.prototype, 'connect')
      .resolves(connectionMock as any)
    sinon
      .stub(Amqp.prototype, 'initialize')
      .resolves(channelMock as any)
    const closeStub = sinon.stub(Amqp.prototype, 'close').rejects(new Error('reconnect failed'))

    await helper.load(
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
      credentialsFixture,
    )

    const onConnClose = connectionMock.on.withArgs('close').getCall(0).args[1]
    await onConnClose().catch(() => undefined)
    await onConnClose().catch(() => undefined)

    expect(closeStub.calledTwice).to.be.true
  })

  it('does not emit unhandled rejection when reconnect fails in connection close handler', async function () {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub() }
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)
    sinon.stub(Amqp.prototype, 'close').rejects(new Error('reconnect failed'))

    await helper.load(
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
      credentialsFixture,
    )

    const n1 = helper.getNode('n1')
    const callErrors: unknown[] = []
    n1.on('call:error', call => {
      callErrors.push(call.args[0])
    })

    let unhandledReason: unknown
    const onUnhandledRejection = (reason: unknown) => {
      unhandledReason = reason
    }
    process.once('unhandledRejection', onUnhandledRejection)
    try {
      const onConnClose = connectionMock.on.withArgs('close').getCall(0).args[1]
      onConnClose()
      await new Promise(resolve => setTimeout(resolve, 50))
    } finally {
      process.removeListener('unhandledRejection', onUnhandledRejection)
    }

    expect(unhandledReason).to.equal(undefined)
    expect(
      callErrors.some(error => /Reconnect failed after connection close/i.test(String(error))),
    ).to.be.true
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

  it('clears pending reconnect timer before switching vhost dynamically', async function () {
    const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true })
    try {
      const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
      const channelMock = { on: sinon.stub(), off: sinon.stub() }

      const connectStub = sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
      sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)
      sinon.stub(Amqp.prototype, 'close').resolves()

      let releaseSetVhost: () => void = () => undefined
      const setVhostPromise = new Promise<void>(resolve => {
        releaseSetVhost = resolve
      })

      sinon.stub(Amqp.prototype, 'setVhost').returns(setVhostPromise as any)
      sinon.stub(Amqp.prototype, 'publish').resolves()
      sinon.stub(Amqp.prototype, 'getConnection').returns(connectionMock as any)
      sinon.stub(Amqp.prototype, 'getChannel').returns(channelMock as any)

      await helper.load(
        [amqpOut, amqpBroker],
        amqpOutFlowFixture,
        credentialsFixture,
      )

      const onConnClose = connectionMock.on.withArgs('close').getCall(0).args[1]
      await onConnClose()

      // Timer is now scheduled for 2000ms.
      await clock.tickAsync(1000)

      const amqpOutNode = helper.getNode('n1')
      amqpOutNode.receive({ payload: 'foo', vhost: 'vh2' })

      // Advance beyond the original reconnect deadline while setVhost is still pending.
      await clock.tickAsync(2000)

      // No reconnect should have fired while the dynamic vhost switch is in progress.
      expect(connectStub.calledOnce).to.be.true

      releaseSetVhost()
      await clock.tickAsync(0)

      expect(connectStub.calledOnce).to.be.true
    } finally {
      clock.restore()
    }
  })

  it('removes broker node state when delete close fails', async function () {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub() }
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)
    sinon.stub(Amqp.prototype, 'close').rejects(new Error('close failed'))
    const removeBrokerNodeStateStub = sinon.stub(Amqp.prototype, 'removeBrokerNodeState')

    await helper.load(
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
      credentialsFixture,
    )
    const amqpOutNode = helper.getNode('n1')
    await amqpOutNode.close(true)

    expect(removeBrokerNodeStateStub.calledOnce).to.be.true
  })

  it('removes broker node state when node is permanently deleted', async function () {
    const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
    const channelMock = { on: sinon.stub(), off: sinon.stub() }
    sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
    sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)
    sinon.stub(Amqp.prototype, 'close').resolves()
    const removeBrokerNodeStateStub = sinon.stub(Amqp.prototype, 'removeBrokerNodeState')

    await helper.load(
      [amqpOut, amqpBroker],
      amqpOutFlowFixture,
      credentialsFixture,
    )
    const amqpOutNode = helper.getNode('n1')
    await amqpOutNode.close(true)

    expect(removeBrokerNodeStateStub.calledOnce).to.be.true
  })

  it('does not schedule reconnect when node closes while reconnect is closing resources', async function () {
    const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true })
    try {
      const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
      const channelMock = { on: sinon.stub(), off: sinon.stub() }
      const connectStub = sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
      sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)
      let releaseClose: () => void = () => undefined
      const closeDuringReconnect = new Promise<void>(resolve => {
        releaseClose = resolve
      })
      const closeStub = sinon.stub(Amqp.prototype, 'close')
      closeStub.onFirstCall().returns(closeDuringReconnect)
      closeStub.onSecondCall().resolves()

      await helper.load(
        [amqpOut, amqpBroker],
        amqpOutFlowFixture,
        credentialsFixture,
      )

      const n1 = helper.getNode('n1')
      const onConnClose = connectionMock.on.withArgs('close').getCall(0).args[1]
      const reconnectPromise = onConnClose()
      await n1.close()
      releaseClose()
      await reconnectPromise
      await clock.tickAsync(2001)

      expect(connectStub.calledOnce).to.be.true
    } finally {
      clock.restore()
    }
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

  it('does not queue duplicate initialization after the reconnect timer fires', async function () {
    const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true })
    try {
      const connectionMock = { on: sinon.stub(), off: sinon.stub(), close: sinon.stub() }
      const channelMock = { on: sinon.stub(), off: sinon.stub() }
      const connectStub = sinon.stub(Amqp.prototype, 'connect').resolves(connectionMock as any)
      const initializeStub = sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)
      sinon
        .stub(Amqp.prototype, 'markConnected')
        .onFirstCall()
        .throws(new Error('hold initial initialization open'))

      let releaseInitialCleanup: () => void = () => undefined
      const initialCleanup = new Promise<void>(resolve => {
        releaseInitialCleanup = resolve
      })
      const closeStub = sinon.stub(Amqp.prototype, 'close')
      closeStub.onFirstCall().returns(initialCleanup)
      closeStub.resolves()

      await helper.load(
        [amqpOut, amqpBroker],
        amqpOutFlowFixture,
        credentialsFixture,
      )

      const onConnClose = connectionMock.on.withArgs('close').getCall(0).args[1]
      await onConnClose()
      await clock.tickAsync(2001)

      await onConnClose()
      await clock.tickAsync(4001)

      releaseInitialCleanup()
      await clock.tickAsync(0)

      expect(connectStub.callCount).to.equal(2)
      expect(initializeStub.callCount).to.equal(2)
    } finally {
      clock.restore()
    }
  })

  it('retries initialization on unclassified connect errors when reconnectOnError is true', async function () {
    const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true })
    try {
      const transientError = new Error('socket hang up')
      const connectStub = sinon.stub(Amqp.prototype, 'connect')
      connectStub.onFirstCall().rejects(transientError)
      connectStub.onSecondCall().resolves({ on: sinon.stub(), off: sinon.stub(), close: sinon.stub() } as any)
      sinon.stub(Amqp.prototype, 'initialize').resolves({ on: sinon.stub(), off: sinon.stub() } as any)
      sinon.stub(Amqp.prototype, 'close').resolves()

      const flow = JSON.parse(JSON.stringify(amqpOutFlowFixture))
      flow[0].reconnectOnError = true

      await helper.load([amqpOut, amqpBroker], flow, credentialsFixture)
      expect(connectStub.callCount).to.equal(1)

      await clock.tickAsync(2001)

      expect(connectStub.callCount).to.equal(2)
    } finally {
      clock.restore()
    }
  })

  it('retries initialization on auth errors when reconnectOnError is true', async function () {
    const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true })
    try {
      const connectStub = sinon.stub(Amqp.prototype, 'connect')
      connectStub.onFirstCall().rejects(new CustomError(ErrorType.InvalidLogin))
      connectStub.onSecondCall().resolves({ on: sinon.stub(), off: sinon.stub(), close: sinon.stub() } as any)
      sinon.stub(Amqp.prototype, 'initialize').resolves({ on: sinon.stub(), off: sinon.stub() } as any)
      sinon.stub(Amqp.prototype, 'close').resolves()

      const flow = JSON.parse(JSON.stringify(amqpOutFlowFixture))
      flow[0].reconnectOnError = true

      await helper.load([amqpOut, amqpBroker], flow, credentialsFixture)
      expect(connectStub.callCount).to.equal(1)

      await clock.tickAsync(2001)

      expect(connectStub.callCount).to.equal(2)
    } finally {
      clock.restore()
    }
  })

  it('does not emit unhandled rejection when reconnect fails during initialization error handling', async function () {
    sinon.stub(Amqp.prototype, 'connect').rejects(new Error('initial connect failed'))
    sinon.stub(Amqp.prototype, 'initialize').resolves({ on: sinon.stub(), off: sinon.stub() } as any)
    sinon.stub(Amqp.prototype, 'close').rejects(new Error('close failed'))

    const flow = JSON.parse(JSON.stringify(amqpOutFlowFixture))
    flow[0].reconnectOnError = true

    let unhandledReason: unknown
    const onUnhandledRejection = (reason: unknown) => {
      unhandledReason = reason
    }
    process.once('unhandledRejection', onUnhandledRejection)
    try {
      await helper.load([amqpOut, amqpBroker], flow, credentialsFixture)
      const amqpOutNode = helper.getNode('n1')
      const callErrors: unknown[] = []
      const callStatuses: unknown[] = []
      amqpOutNode.on('call:error', call => {
        callErrors.push(call.args[0])
      })
      amqpOutNode.on('call:status', call => {
        callStatuses.push(call.args[0])
      })

      await new Promise(resolve => setTimeout(resolve, 50))

      expect(unhandledReason).to.equal(undefined)
      expect(
        callErrors.some(error => /Reconnect failed during initialization/i.test(String(error))),
      ).to.be.true
      expect(
        callStatuses.some(
          status => status && typeof status === 'object' && (status as { text?: string }).text === NODE_STATUS.Error.text,
        ),
      ).to.be.true
    } finally {
      process.removeListener('unhandledRejection', onUnhandledRejection)
    }
  })

  it('backs off repeated reconnect initialization failures', async function () {
    const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true })
    try {
      const transientError = new Error('socket hang up')
      const connectStub = sinon.stub(Amqp.prototype, 'connect')
      connectStub.onFirstCall().rejects(transientError)
      connectStub.onSecondCall().rejects(transientError)
      connectStub.onThirdCall().resolves({ on: sinon.stub(), off: sinon.stub(), close: sinon.stub() } as any)
      sinon.stub(Amqp.prototype, 'initialize').resolves({ on: sinon.stub(), off: sinon.stub() } as any)
      sinon.stub(Amqp.prototype, 'close').resolves()

      const flow = JSON.parse(JSON.stringify(amqpOutFlowFixture))
      flow[0].reconnectOnError = true

      await helper.load([amqpOut, amqpBroker], flow, credentialsFixture)
      expect(connectStub.callCount).to.equal(1)

      await clock.tickAsync(2001)
      expect(connectStub.callCount).to.equal(2)

      await clock.tickAsync(2000)
      expect(connectStub.callCount).to.equal(2)

      await clock.tickAsync(2000)
      expect(connectStub.callCount).to.equal(3)
    } finally {
      clock.restore()
    }
  })

  it('keeps retrying after broker-close recovery fails with reconnectOnError disabled', async function () {
    const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true })
    try {
      const initialConnection = {
        on: sinon.stub(),
        off: sinon.stub(),
        close: sinon.stub(),
      }
      const recoveredConnection = {
        on: sinon.stub(),
        off: sinon.stub(),
        close: sinon.stub(),
      }
      const channelMock = { on: sinon.stub(), off: sinon.stub() }
      const connectStub = sinon.stub(Amqp.prototype, 'connect')
      connectStub.onFirstCall().resolves(initialConnection as any)
      connectStub
        .onSecondCall()
        .rejects(new Error('broker is still restarting'))
      connectStub.onThirdCall().resolves(recoveredConnection as any)
      sinon.stub(Amqp.prototype, 'initialize').resolves(channelMock as any)
      sinon.stub(Amqp.prototype, 'close').resolves()

      const flow = JSON.parse(JSON.stringify(amqpOutFlowFixture))
      flow[0].reconnectOnError = false

      await helper.load([amqpOut, amqpBroker], flow, credentialsFixture)
      const onConnectionClose = initialConnection.on
        .withArgs('close')
        .getCall(0).args[1]

      await onConnectionClose()
      await clock.tickAsync(2001)
      expect(connectStub.callCount).to.equal(2)

      await clock.tickAsync(4001)
      expect(connectStub.callCount).to.equal(3)
    } finally {
      clock.restore()
    }
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
