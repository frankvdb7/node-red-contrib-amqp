import { expect } from 'chai'
import { EventEmitter } from 'events'
import trackTerminalClose from '../../src/nodes/flow-close-tracker'

describe('flow close tracker', () => {
  it('removes bindings for a runtime child of a changed subflow instance', () => {
    const events = new EventEmitter()
    const terminalClose = trackTerminalClose(
      { events } as never,
      'subflow-instance-template-amqp-in',
    )

    events.emit('flows:stopping', {
      diff: { changed: ['subflow-instance'], removed: [] },
    })

    expect(terminalClose.shouldRemoveBindings(false)).to.be.true
    terminalClose.dispose()
  })

  it('does not match an unrelated subflow instance with a similar prefix', () => {
    const events = new EventEmitter()
    const terminalClose = trackTerminalClose(
      { events } as never,
      'subflow-instance2-template-amqp-in',
    )

    events.emit('flows:stopping', {
      diff: { changed: ['subflow-instance'], removed: [] },
    })

    expect(terminalClose.shouldRemoveBindings(false)).to.be.false
    terminalClose.dispose()
  })
})
