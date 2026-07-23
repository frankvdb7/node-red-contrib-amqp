import { NodeRedApp } from 'node-red'

interface FlowStoppingEvent {
  diff?: {
    changed?: string[]
    removed?: string[]
  }
}

interface TrackedNode {
  nodeId: string
  terminalClose: boolean
}

interface TrackerState {
  nodes: Map<symbol, TrackedNode>
  onFlowsStopping: (event: FlowStoppingEvent) => void
}

const trackerStates = new WeakMap<object, TrackerState>()

export default function trackTerminalClose(
  RED: NodeRedApp,
  nodeId: string,
): {
  dispose: () => void
  shouldRemoveBindings: (removed: boolean) => boolean
} {
  let state = trackerStates.get(RED.events)
  if (!state) {
    const nodes = new Map<symbol, TrackedNode>()
    const onFlowsStopping = (event: FlowStoppingEvent): void => {
      for (const trackedNode of nodes.values()) {
        trackedNode.terminalClose ||= Boolean(
          event.diff?.changed?.includes(trackedNode.nodeId) ||
            event.diff?.removed?.includes(trackedNode.nodeId),
        )
      }
    }
    state = { nodes, onFlowsStopping }
    trackerStates.set(RED.events, state)
    RED.events.on('flows:stopping', onFlowsStopping)
  }

  const token = Symbol(nodeId)
  const trackedNode = { nodeId, terminalClose: false }
  state.nodes.set(token, trackedNode)
  let disposed = false

  return {
    dispose: () => {
      if (disposed) {
        return
      }
      disposed = true
      state.nodes.delete(token)
      if (state.nodes.size === 0) {
        RED.events.removeListener('flows:stopping', state.onFlowsStopping)
        if (trackerStates.get(RED.events) === state) {
          trackerStates.delete(RED.events)
        }
      }
    },
    shouldRemoveBindings: removed => removed || trackedNode.terminalClose,
  }
}
