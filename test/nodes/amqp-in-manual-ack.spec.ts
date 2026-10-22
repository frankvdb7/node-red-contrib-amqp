/* eslint-disable @typescript-eslint/no-var-requires */
/* eslint-disable @typescript-eslint/ban-ts-comment */
export {}
const { expect } = require('chai')
const sinon = require('sinon')
const Amqp = require('../../src/Amqp').default
const { ManualAckType, NodeType } = require('../../src/types')
const {
  amqpInManualAckFlowFixture,
  credentialsFixture,
} = require('../doubles')
const helper = require('node-red-node-test-helper')
const amqpInManualAck = require('../../src/nodes/amqp-in-manual-ack')
const amqpBroker = require('../../src/nodes/amqp-broker')

helper.init(require.resolve('node-red'))

describe('amqp-in-manual-ack Node', () => {
  beforeEach(function (done) {
    sinon.stub(Amqp.prototype, 'connect').resolves()
    helper.startServer(done)
  })

  afterEach(function (done) {
    helper.unload()
    helper.stopServer(done)
    sinon.restore()
  })

  it('should be loaded', done => {
    const flow = [
      { id: 'n1', type: NodeType.AmqpInManualAck, name: 'test name' },
    ]
    helper.load(amqpInManualAck, flow, () => {
      const n1 = helper.getNode('n1')
      n1.should.have.property('name', 'test name')
      done()
    })
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

})
