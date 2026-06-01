/* eslint-disable @typescript-eslint/no-var-requires */
export {}
const { expect } = require('chai')
const ReconnectBackoff = require('../src/reconnect-backoff').default

describe('ReconnectBackoff', () => {
  it('caps reconnect delay at five minutes', () => {
    const backoff = new ReconnectBackoff()
    const delays = Array.from({ length: 12 }, () => backoff.nextDelayMs())

    expect(delays).to.deep.equal([
      2000,
      4000,
      8000,
      16000,
      32000,
      64000,
      128000,
      256000,
      300000,
      300000,
      300000,
      300000,
    ])
  })

  it('resets reconnect delay to the initial value', () => {
    const backoff = new ReconnectBackoff()

    backoff.nextDelayMs()
    backoff.nextDelayMs()
    backoff.reset()

    expect(backoff.nextDelayMs()).to.equal(2000)
  })
})
