export {}
const { expect } = require('chai')
const sinon = require('sinon')
const AmqpLifecycleCoordinator =
  require('../src/amqp-lifecycle-coordinator').default

describe('AmqpLifecycleCoordinator', () => {
  afterEach(() => {
    sinon.restore()
  })

  const createNode = () => ({
    log: sinon.stub(),
  })

  it('aborts a stalled replacement operation during shutdown', async () => {
    const close = sinon.stub().resolves()
    const coordinator = new AmqpLifecycleCoordinator({
      node: createNode(),
      initialize: sinon.stub().resolves(),
      close,
      removeEventListeners: sinon.stub(),
      clearResources: sinon.stub(),
      onInitializationError: sinon.stub().resolves(),
    })
    await coordinator.start()

    let replacementSignal: AbortSignal | undefined
    let notifyStarted: () => void = () => undefined
    const started = new Promise<void>(resolve => {
      notifyStarted = resolve
    })
    const replacement = coordinator.replace(
      new Error('replace current lifecycle'),
      async ({ signal }: { signal: AbortSignal }) => {
        replacementSignal = signal
        notifyStarted()
        await new Promise<void>((_resolve, reject) => {
          signal.addEventListener(
            'abort',
            () => reject(signal.reason),
            { once: true },
          )
        })
      },
    )
    await started

    coordinator.shutdown(new Error('node shutdown'))

    let replacementError: Error | undefined
    await replacement.catch((error: Error) => {
      replacementError = error
    })
    expect(replacementSignal?.aborted).to.equal(true)
    expect(replacementError?.message).to.equal('node shutdown')
  })

  it('does not start scheduled reconnect initialization after shutdown', async () => {
    const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true })
    try {
      const initialize = sinon.stub().resolves()
      const coordinator = new AmqpLifecycleCoordinator({
        node: createNode(),
        initialize,
        close: sinon.stub().resolves(),
        removeEventListeners: sinon.stub(),
        clearResources: sinon.stub(),
        onInitializationError: sinon.stub().resolves(),
      })
      await coordinator.start()
      await coordinator.reconnect()

      coordinator.shutdown(new Error('node shutdown'))
      await clock.tickAsync(2001)

      expect(initialize.calledOnce).to.equal(true)
    } finally {
      clock.restore()
    }
  })

  it('coalesces reconnect requests through one shared backoff timer', async () => {
    const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true })
    try {
      const initialize = sinon.stub().resolves()
      const close = sinon.stub().resolves()
      const coordinator = new AmqpLifecycleCoordinator({
        node: createNode(),
        initialize,
        close,
        removeEventListeners: sinon.stub(),
        clearResources: sinon.stub(),
        onInitializationError: sinon.stub().resolves(),
      })
      await coordinator.start()

      await Promise.all([coordinator.reconnect(), coordinator.reconnect()])
      await clock.tickAsync(2001)

      expect(close.calledOnce).to.equal(true)
      expect(initialize.calledTwice).to.equal(true)
    } finally {
      clock.restore()
    }
  })
})
