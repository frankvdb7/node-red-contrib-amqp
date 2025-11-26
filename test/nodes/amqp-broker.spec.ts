/* eslint-disable @typescript-eslint/no-var-requires */
/* eslint-disable @typescript-eslint/ban-ts-comment */
export {}
const { expect } = require('chai')
const sinon = require('sinon')
const helper = require('node-red-node-test-helper')
const amqpBroker = require('../../src/nodes/amqp-broker')

helper.init(require.resolve('node-red'))

describe('amqp-broker Node', () => {
  beforeEach(function (done) {
    helper.startServer(done)
  })

  afterEach(function (done) {
    helper.unload()
    helper.stopServer(done)
    sinon.restore()
  })

  describe('Health Check Endpoint', () => {
    it('should return 200 when all brokers are connected', done => {
      const flow = [
        {
          id: 'b1',
          type: 'amqp-broker',
          name: 'broker 1',
          connections: { n1: true },
        },
        {
          id: 'b2',
          type: 'amqp-broker',
          name: 'broker 2',
          connections: { n2: true },
        },
      ]
      helper.load(amqpBroker, flow, () => {
        helper
          .request()
          .get('/amqp-broker/health')
          .expect(200)
          .end((err, res) => {
            if (err) return done(err)
            expect(res.body).to.deep.equal({
              overallStatus: 'healthy',
              brokers: [
                { id: 'b1', name: 'broker 1', status: 'connected' },
                { id: 'b2', name: 'broker 2', status: 'connected' },
              ],
            })
            done()
          })
      })
    })

    it('should return 503 when one broker is disconnected', done => {
      const flow = [
        {
          id: 'b1',
          type: 'amqp-broker',
          name: 'broker 1',
          connections: { n1: true },
        },
        {
          id: 'b2',
          type: 'amqp-broker',
          name: 'broker 2',
          connections: { n2: false },
        },
      ]
      helper.load(amqpBroker, flow, () => {
        helper
          .request()
          .get('/amqp-broker/health')
          .expect(503)
          .end((err, res) => {
            if (err) return done(err)
            expect(res.body).to.deep.equal({
              overallStatus: 'unhealthy',
              brokers: [
                { id: 'b1', name: 'broker 1', status: 'connected' },
                { id: 'b2', name: 'broker 2', status: 'disconnected' },
              ],
            })
            done()
          })
      })
    })

    it('should return 503 when a broker has no connections property', done => {
      const flow = [
        {
          id: 'b1',
          type: 'amqp-broker',
          name: 'broker 1',
          connections: { n1: true },
        },
        {
          id: 'b2',
          type: 'amqp-broker',
          name: 'broker 2',
        },
      ]
      helper.load(amqpBroker, flow, () => {
        helper
          .request()
          .get('/amqp-broker/health')
          .expect(503)
          .end((err, res) => {
            if (err) return done(err)
            expect(res.body).to.deep.equal({
              overallStatus: 'unhealthy',
              brokers: [
                { id: 'b1', name: 'broker 1', status: 'connected' },
                { id: 'b2', name: 'broker 2', status: 'disconnected' },
              ],
            })
            done()
          })
      })
    })
  })
})
