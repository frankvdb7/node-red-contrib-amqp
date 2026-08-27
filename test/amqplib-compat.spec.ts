/* eslint-disable @typescript-eslint/no-explicit-any */
export {}

const { expect } = require('chai')
const net = require('node:net')
const amqplib = require('amqplib')

const FRAME_END = 0xce
const PROTOCOL_HEADER = Buffer.from('AMQP\0\0\x09\x01', 'binary')

function methodFrame(method: number, fields: Buffer, channel = 0): Buffer {
  const payload = Buffer.alloc(4 + fields.length)
  payload.writeUInt16BE(10, 0)
  payload.writeUInt16BE(method, 2)
  fields.copy(payload, 4)

  const header = Buffer.alloc(7)
  header.writeUInt8(1, 0)
  header.writeUInt16BE(channel, 1)
  header.writeUInt32BE(payload.length, 3)

  return Buffer.concat([header, payload, Buffer.from([FRAME_END])])
}

function longString(value: string): Buffer {
  const encoded = Buffer.from(value)
  const length = Buffer.alloc(4)
  length.writeUInt32BE(encoded.length, 0)
  return Buffer.concat([length, encoded])
}

function startFrame(): Buffer {
  const fields = Buffer.concat([
    Buffer.from([0, 9]),
    Buffer.alloc(4),
    longString('PLAIN'),
    longString('en_US'),
  ])
  return methodFrame(10, fields)
}

function tuneFrame(heartbeat: number): Buffer {
  const fields = Buffer.alloc(8)
  fields.writeUInt16BE(0, 0)
  fields.writeUInt32BE(131072, 2)
  fields.writeUInt16BE(heartbeat, 6)
  return methodFrame(30, fields)
}

function openOkFrame(): Buffer {
  return methodFrame(41, Buffer.from([0]))
}

function closeOkFrame(): Buffer {
  return methodFrame(51, Buffer.alloc(0))
}

async function createAmqpProbe(serverHeartbeat: number) {
  let resolveNegotiatedHeartbeat: (heartbeat: number) => void
  let rejectNegotiatedHeartbeat: (error: Error) => void
  const negotiatedHeartbeat = new Promise<number>((resolve, reject) => {
    resolveNegotiatedHeartbeat = resolve
    rejectNegotiatedHeartbeat = reject
  })

  const sockets = new Set<any>()
  const server = net.createServer((socket: any) => {
    sockets.add(socket)
    let data = Buffer.alloc(0)
    let startSent = false

    socket.on('data', (chunk: Buffer) => {
      data = Buffer.concat([data, chunk])

      if (!startSent && data.length >= PROTOCOL_HEADER.length) {
        expect(data.subarray(0, PROTOCOL_HEADER.length)).to.deep.equal(
          PROTOCOL_HEADER,
        )
        data = data.subarray(PROTOCOL_HEADER.length)
        socket.write(startFrame())
        startSent = true
      }

      while (data.length >= 8) {
        const frameSize = data.readUInt32BE(3)
        const frameLength = 7 + frameSize + 1
        if (data.length < frameLength) {
          return
        }

        const payload = data.subarray(7, 7 + frameSize)
        expect(data[0]).to.equal(1)
        expect(data[7 + frameSize]).to.equal(FRAME_END)
        data = data.subarray(frameLength)

        const method = payload.readUInt16BE(2)
        if (method === 11) {
          socket.write(tuneFrame(serverHeartbeat))
        } else if (method === 31) {
          resolveNegotiatedHeartbeat(payload.readUInt16BE(10))
        } else if (method === 40) {
          socket.write(openOkFrame())
        } else if (method === 50) {
          socket.write(closeOkFrame())
        }
      }
    })

    socket.on('error', rejectNegotiatedHeartbeat)
    socket.on('close', () => sockets.delete(socket))
  })

  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', resolve)
  })

  const address = server.address()
  const port = typeof address === 'object' && address ? address.port : undefined
  if (!port) {
    throw new Error('AMQP probe did not receive a listening port')
  }

  return {
    negotiatedHeartbeat,
    port,
    async close(): Promise<void> {
      for (const socket of sockets) {
        socket.destroy()
      }
      await new Promise<void>(resolve => server.close(() => resolve()))
    },
  }
}

describe('amqplib compatibility', () => {
  it('uses heartbeat 0 to disable the broker-suggested heartbeat', async function () {
    this.timeout(5000)
    const probe = await createAmqpProbe(60)

    try {
      const connection = await amqplib.connect(
        `amqp://127.0.0.1:${probe.port}?heartbeat=0`,
      )

      expect(await probe.negotiatedHeartbeat).to.equal(0)
      await connection.close()
    } finally {
      await probe.close()
    }
  })
})
