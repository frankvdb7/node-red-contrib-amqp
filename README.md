

# source
This repo fork from [@Stormpass/node-red-contrib-amqp](https://github.com/Stormpass/node-red-contrib-amqp) 

and

+ fixed error on node flow save
+ The nodes are adapted to work with RabbitMQ and their newer Quorom type queues. You can set the additional queue options from the node configuration.

AMQP nodes for node-red (back pushed changes from @mnn-o/node-red-rabbitmq)

## Installation

Install via the Palette Manager or from within your node-red directory (typically `~/.node-red`) run:

```
npm i @frankvdb/node-red-contrib-amqp
```

## Usage

Provides three nodes and an amqp broker config node.
Please see the `Node Help` section from within node-red for more info

### Dynamic Virtual Host

The virtual host used by the `amqp-out` node can be changed at runtime by setting `msg.vhost` on the incoming message. When provided, the node will reconnect to the specified RabbitMQ virtual host before publishing the message. Connections are pooled per virtual host so nodes targeting the same vhost share a single connection, while nodes targeting different vhosts operate independently.

**Note:** This action modifies the shared broker configuration. Any other nodes using the same broker will also connect to the new virtual host on their next reconnection.

## Development

### Build the project

```
npm run build
```

### Run tests

```
npm test
```

Run coverage:

```
npm run test:cov
```

