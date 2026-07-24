import { Node, NodeRedApp } from 'node-red'

interface FlowStoppingEvent {
  diff?: {
    changed?: string[]
    removed?: string[]
  }
}

interface TrackedNode {
  nodeId: string
  subflowOwnerIds: Set<string>
  terminalClose: boolean
}

interface RuntimeFlow {
  TYPE?: string
  id?: string
  parent?: RuntimeFlow
}

type RuntimeNode = Node & {
  _flow?: RuntimeFlow
}

interface TrackerState {
  nodes: Map<symbol, TrackedNode>
  onFlowsStopping: (event: FlowStoppingEvent) => void
}

const trackerStates = new WeakMap<object, TrackerState>()

function containsNodeOrOwnedSubflow(
  nodeIds: string[] | undefined,
  trackedNode: TrackedNode,
): boolean {
  return Boolean(
    nodeIds?.some(
      changedNodeId =>
        trackedNode.nodeId === changedNodeId ||
        trackedNode.subflowOwnerIds.has(changedNodeId),
    ),
  )
}

function getSubflowOwnerIds(node: RuntimeNode): Set<string> {
  const ownerIds = new Set<string>()
  const visited = new Set<RuntimeFlow>()
  let flow: RuntimeFlow | undefined = node._flow
  while (flow && !visited.has(flow)) {
    visited.add(flow)
    if (
      flow.id &&
      (flow.TYPE === 'subflow' || flow.TYPE?.startsWith('module:'))
    ) {
      ownerIds.add(flow.id)
    }
    flow = flow.parent
  }
  return ownerIds
}

export default function trackTerminalClose(
  RED: NodeRedApp,
  node: RuntimeNode,
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
          containsNodeOrOwnedSubflow(
            event.diff?.changed,
            trackedNode,
          ) ||
            containsNodeOrOwnedSubflow(
              event.diff?.removed,
              trackedNode,
            ),
        )
      }
    }
    state = { nodes, onFlowsStopping }
    trackerStates.set(RED.events, state)
    RED.events.on('flows:stopping', onFlowsStopping)
  }

  const token = Symbol(node.id)
  const trackedNode = {
    nodeId: node.id,
    subflowOwnerIds: getSubflowOwnerIds(node),
    terminalClose: false,
  }
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
