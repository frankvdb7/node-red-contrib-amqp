import { expect } from 'chai'
import { EventEmitter } from 'events'
import trackTerminalClose from '../../src/nodes/flow-close-tracker'

describe('flow close tracker', () => {
  it('removes bindings for a runtime child of a changed subflow instance', () => {
    const events = new EventEmitter()
    const terminalClose = trackTerminalClose(
      { events } as never,
      {
        id: 'subflow-instance-template-amqp-in',
        _flow: { TYPE: 'subflow', id: 'subflow-instance' },
      } as never,
    )

    events.emit('flows:stopping', {
      diff: { changed: ['subflow-instance'], removed: [] },
    })

    expect(terminalClose.shouldRemoveBindings(false)).to.be.true
    terminalClose.dispose()
  })

  it('does not treat an arbitrarily prefixed node as a subflow child', () => {
    const events = new EventEmitter()
    const terminalClose = trackTerminalClose(
      { events } as never,
      {
        id: 'abc-def',
        _flow: { TYPE: 'flow', id: 'flow-1' },
      } as never,
    )

    events.emit('flows:stopping', {
      diff: { changed: ['abc'], removed: [] },
    })

    expect(terminalClose.shouldRemoveBindings(false)).to.be.false
    terminalClose.dispose()
  })
})
