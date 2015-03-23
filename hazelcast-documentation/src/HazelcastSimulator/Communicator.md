

## Communicator

Communicator enables message passing to Agents, Workers and Tests. You can use messages to simulate various conditions such as network partitioning, high CPU utilization and generally create a discomfort for Hazelcast.

### Example

```
$ communicator --message-address Agent=*,Worker=* spinCore
```
This will send a message `spinCore` to all Workers.


Each interaction with Communicator has to specify:

- Message Type
- Message Address

### Message Types

- `kill` - Kills a JVM running a message recipient. In practice, you probably want to send this message to Worker(s) only as you rarely want to kill an Agent and it does not make sense to send this to just a single test - it would kill other tests sharing the same JVM as well.
- `blockHzTraffic` - Blocks the incoming traffic to TCP port range 5700:5800.
- `newMember` - Starts a new member. You can send this message to Agents only.
- `softKill` - Instructs a JVM running a message recipient to exit.
- `spinCore` - Starts a new busy-spinning thread. You can use it to simulate increased CPU consumption.
- `unblockTraffic` - Open ports blocked by the `blockHzTraffic` message.
- `oom` - Forces a message recipient use all memory and cause OutOfMemoryError.
- `terminateWorker` - Terminates a random Worker. This message type can be targeted to an Agent only.

### Message Addressing

You can send a message to Agent, Worker or Test. These resources create a naturally hierarchy hence the messaging address is hierarchical as well.

Syntax: `Agent=<mode>[,Worker=<mode>[,Test=<mode>]]`.

Mode can be either '*' for broadcast or 'R' for a single random destination.

**Addressing Example 1:**

`Agent=*,Worker=R`: A message will be routed to all agents and then each agent will pass it to a single random worker for processing.

**Addressing Example 2:**

`Agent=*,Worker=R,Test=*`: A message will be routed to all agents, then each agent will pass the message to a single random worker and workers will pass the message to all tests for processing.

#### Addressing shortcuts

Hierarchical addressing is powerful, but it can be quite verbose. Therefore, there are convenient shortcuts you can use as shown below.

- `--oldest-member`: Sends a message to a worker with the oldest cluster member.
- `--random-agent`: Sends a message to a random agent.
- `--random-worker`: Sends a message to a random worker.

**Example:**
The following command starts a busy-spinning thread in a JVM running a random Worker.

```
communicator --random-worker spinCore
```

