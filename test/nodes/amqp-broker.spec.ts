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
    helper.unload().then(() => {
      helper.stopServer(done)
    }).catch(() => {
      helper.stopServer(done)
    })
    sinon.restore()
  })

  describe('Health Check Endpoint', () => {
    it('should return 503 when no broker nodes are configured', done => {
      helper.load(amqpBroker, [], () => {
        helper
          .request()
          .get('/amqp-broker/health')
          .expect(503)
          .end((err, res) => {
            try {
              if (err) return done(err)
              expect(res.body).to.deep.equal({
                overallStatus: 'unhealthy',
                brokers: [],
              })
              done()
            } catch (e) {
              done(e)
            }
          })
      })
    })

    it('should return 200 when all brokers are connected', done => {
      const flow = [
        {
          id: 'b1',
          type: 'amqp-broker',
          name: 'broker 1',
          nodeStates: { n1: 'connected' },
        },
        {
          id: 'b2',
          type: 'amqp-broker',
          name: 'broker 2',
          nodeStates: { n2: 'connected' },
        },
      ]
      helper.load(amqpBroker, flow, () => {
        helper
          .request()
          .get('/amqp-broker/health')
          .expect(200)
          .end((err, res) => {
            try {
              if (err) return done(err)
              expect(res.body).to.deep.equal({
                overallStatus: 'healthy',
                brokers: [
                  { id: 'b1', name: 'broker 1', status: 'connected' },
                  { id: 'b2', name: 'broker 2', status: 'connected' },
                ],
              })
              done()
            } catch (e) {
              done(e)
            }
          })
      })
    })

    it('should return 503 when one broker is disconnected', done => {
      const flow = [
        {
          id: 'b1',
          type: 'amqp-broker',
          name: 'broker 1',
          nodeStates: { n1: 'connected' },
        },
        {
          id: 'b2',
          type: 'amqp-broker',
          name: 'broker 2',
          nodeStates: { n2: 'disconnected' },
        },
      ]
      helper.load(amqpBroker, flow, () => {
        helper
          .request()
          .get('/amqp-broker/health')
          .expect(503)
          .end((err, res) => {
            try {
              if (err) return done(err)
              expect(res.body).to.deep.equal({
                overallStatus: 'unhealthy',
                brokers: [
                  { id: 'b1', name: 'broker 1', status: 'connected' },
                  { id: 'b2', name: 'broker 2', status: 'disconnected' },
                ],
              })
              done()
            } catch(e) {
              done(e)
            }
          })
      })
    })

    it('should return 503 when a broker has an errored node state', done => {
      const flow = [
        {
          id: 'b1',
          type: 'amqp-broker',
          name: 'broker 1',
          nodeStates: { n1: 'errored' },
          lastError: {
            n1: {
              message: 'Broker auth failed',
              code: 'EACCES',
              at: '2026-03-02T00:00:00.000Z',
            },
          },
        },
      ]
      helper.load(amqpBroker, flow, () => {
        helper
          .request()
          .get('/amqp-broker/health')
          .expect(503)
          .end((err, res) => {
            try {
              if (err) return done(err)
              expect(res.body).to.deep.equal({
                overallStatus: 'unhealthy',
                brokers: [
                  {
                    id: 'b1',
                    name: 'broker 1',
                    status: 'errored',
                    lastError: {
                      n1: {
                        message: 'Broker auth failed',
                        code: 'EACCES',
                        at: '2026-03-02T00:00:00.000Z',
                      },
                    },
                  },
                ],
              })
              done()
            } catch (e) {
              done(e)
            }
          })
      })
    })

    it('should return 503 with disconnected status and lastError when a connection closes unexpectedly', done => {
      const flow = [
        {
          id: 'b1',
          type: 'amqp-broker',
          name: 'broker 1',
          nodeStates: { n1: 'disconnected' },
          lastError: {
            n1: {
              message: 'AMQP connection closed',
              at: '2026-03-02T00:00:00.000Z',
            },
          },
        },
      ]
      helper.load(amqpBroker, flow, () => {
        helper
          .request()
          .get('/amqp-broker/health')
          .expect(503)
          .end((err, res) => {
            try {
              if (err) return done(err)
              expect(res.body).to.deep.equal({
                overallStatus: 'unhealthy',
                brokers: [
                  {
                    id: 'b1',
                    name: 'broker 1',
                    status: 'disconnected',
                    lastError: {
                      n1: {
                        message: 'AMQP connection closed',
                        at: '2026-03-02T00:00:00.000Z',
                      },
                    },
                  },
                ],
              })
              done()
            } catch (e) {
              done(e)
            }
          })
      })
    })

    it('should return 503 when a broker has no nodeStates property', done => {
      const flow = [
        {
          id: 'b1',
          type: 'amqp-broker',
          name: 'broker 1',
          nodeStates: { n1: 'connected' },
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
            try {
              if (err) return done(err)
              expect(res.body).to.deep.equal({
                overallStatus: 'unhealthy',
                brokers: [
                  { id: 'b1', name: 'broker 1', status: 'connected' },
                  { id: 'b2', name: 'broker 2', status: 'disconnected' },
                ],
              })
              done()
            } catch(e) {
              done(e)
            }
          })
      })
    })

    it('should reflect flow redeployment', (done) => {
      const flow1 = [
        { id: 'b1', type: 'amqp-broker', name: 'broker 1', nodeStates: { n1: 'connected' } }
      ]
      const flow2 = [
        { id: 'b2', type: 'amqp-broker', name: 'broker 2', nodeStates: { n2: 'connected' } }
      ]

      helper.load(amqpBroker, flow1, () => {
        helper.request().get('/amqp-broker/health').end((err1, res1) => {
          try {
            if (err1) return done(err1)
            expect(res1.body.brokers).to.have.lengthOf(1)
            expect(res1.body.brokers[0].id).to.equal('b1')

            helper.unload().then(() => {
              helper.load(amqpBroker, flow2, () => {
                helper.request().get('/amqp-broker/health').end((err2, res2) => {
                  try {
                    if (err2) return done(err2)
                    expect(res2.body.brokers).to.have.lengthOf(1)
                    expect(res2.body.brokers[0].id).to.equal('b2')
                    done()
                  } catch(e) {
                    done(e)
                  }
                })
              })
            })
          } catch(e) {
            done(e)
          }
        })
      })
    })

    it('should reflect the removal of a broker', done => {
      const flow = [
        {
          id: 'b1',
          type: 'amqp-broker',
          name: 'broker 1',
          nodeStates: { n1: 'connected' },
        },
        {
          id: 'b2',
          type: 'amqp-broker',
          name: 'broker 2',
          nodeStates: { n2: 'connected' },
        },
      ]
      helper.load(amqpBroker, flow, () => {
        const b2 = helper.getNode('b2')
        b2.close()
        // No setTimeout needed, the 'close' event is synchronous
        helper
          .request()
          .get('/amqp-broker/health')
          .expect(200)
          .end((err, res) => {
            try {
              if (err) return done(err)
              expect(res.body.brokers).to.have.lengthOf(1)
              expect(res.body.brokers[0]).to.deep.equal({
                id: 'b1',
                name: 'broker 1',
                status: 'connected',
              })
              done()
            } catch(e) {
              done(e)
            }
          })
      })
    })
  })
})
