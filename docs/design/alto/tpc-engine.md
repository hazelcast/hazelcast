# Thread-per-core Engine

## Reactor

The basis of the thread-per-core engine is the reactor; which is an
implementation of the reactor-design-pattern. Inside the reactor, there
runs an eventloop and a single eventloop-thread. The primary task of the
reactor is creating/managing the eventloop (and eventloop thread) and
offering work to the eventloop like an external task that needs to be
processed on the eventloop. Interaction with the reactor is thread-safe.

A reactor is configured/created using the ReactorBuilder. There is a
ReactorBuilder for NIO, and there will be a ReactorBuilder for io_uring
because each reactor can have its own implementation-specific
properties.

The eventloop thread can be pinned to a core or a set of cores, but that
isn’t done by default.

## Eventloop

The eventloop is a loop which processes events. This can be I/O events,
but also ‘tasks’. It is very similar to the eventloop in Javascript. The
eventloop will wait for async I/O and tasks ‘events’ and will call the
appropriate handler for that event and just run the tasks (like an
executor).

The eventloop should only be accessed from the owning event-loop-thread
because the Eventloop isn’t thread-safe.

The eventloop also has the ability for task scheduling:

1. Schedule a task to run at some point in time
2. Schedule a task to be run with a fixed delay or fixed frequency

These tasks are run on the event loop-thread itself (since we do not
want other threads).

The eventloop is specific to the async I/O library used. In the 5.3
release, we ship the NioEventloop (and NioReactor) which can only do
networking. On Linux, the NioEventloop is EPoll based and the
non-blocking I/O functionality is provided by the java.nio.Selector.

In the 5.4 release, we will also add the IOUringEventloop (and
IOUringReactor) that can do both networking and storage and will rely on
the io_uring for non-blocking I/O. Probably the IOUringReactor will be
part of the Hazelcast enterprise. The IOUringEventloop is specific to
Linux and requires linux 5.7+.

The eventloop can spin instead of block on the I/O multiplexer, but that
isn’t done by default.

## Promise

You never want to block the Eventloop because it will stall anything
running on top of this eventloop. So anything that otherwise would block
needs to be done asynchronously, and you need to have some kind of
mechanism to continue once some async task completes. That is the task
of the promise. The Promise is the TPC version of the CompletableFuture.

The Promise can only be used within a single eventloop and isn’t
threadsafe. Promises support pooling to prevent create litter on the
fast path.

## TPCEngine

The TPCEngine is effectively nothing else than an array of reactors. It
is mostly a matter of convenience, it doesn't provide much more
functionality apart from creation, starting, and stopping.

Reactors can be used directly without the need for a TPCEngine. This
makes testing a lot less cumbersome.

The functionality of the tpc-engine doesn't know anything about
functionality running on top like Alto or Hazelcast classic.

The TpcEngine is created using the TpcEngineBuilder.

## Scheduler

The Eventloop will receive work e.g. a put-request coming from an
AsyncSocket. This work is offered to the scheduler. And the eventloop
gives the scheduler a tick on every iteration of the loop so the
scheduler can do some work. E.g. it could process 10 requests and then
hand back control to the eventloop. The scheduler is the point where you
can plug in operation execution etc.

## AsyncFile, AsyncSocket, AsyncServerSocket

To prevent dealing with handlers in the reactor directly, there are 3
abstractions that give a much more intuitive programming model:

- AsyncSocket: a non-blocking version of a regular socket. So all reads
  and writes to the socket are non-blocking. The NioReactor will provide
  a NioAsyncSocket and the IOUringReactor will provide the
  IOUringAsyncSocket.
- AsyncServerSocket: a non-blocking version of a server socket, so you
  can accept incoming socket connections. The NioReactor will provide
  the NioAsyncServerSocket and The IOUringReactor provides the
  IOUringAsyncServerSocket.
- AsyncFile: a non-blocking version of a file. All interaction with this
  file will be non-blocking. The big advantage is that you can have many
  concurrent IO operations in flight and this makes it possible to fully
  utilize modern NVMe based devices. A single Reactor for example can
  easily do 400K IOPS with 64 concurrent requests. Currently, we only
  have an IOUringAsyncFile. In the future, we’ll probably add a
  NioAsyncFile. Even though there is no true async I/O possible with
  NIO, we’ll simulate it e.g. by having a threadpool of blocking I/O
  calls. This will be a lot less efficient than I/O uring, but can be
  used as a fallback option in case io_uring isn’t available (e.g.
  Windows/OSX etc., or old Linux distro). The AsyncFile will be added to
  HZ 5.4.

Each eventloop can handle many
AsyncSockets/AsyncServerSockets/AsyncFiles, but each
AsyncSocket/AsyncServerSocket/AsyncFile belongs to a single reactor.

AsyncSocket and AsyncServerSocket are created using AsyncSocketBuilder
and AsyncServerSocketBuilder. The big advantage of this approach is
that most of the complexities of configuring the Async(Server)Socket can
be moved to builders and seriously simplify the Async(Server)Socket.

## ReadHandler

Every AsyncSocket has a readHandler. The readHandler gets notified as
soon as the AsyncSocket has received some data. This is where a
ClientMessage or Packet object can be created and fed into the scheduler
for later processing.

## IOBuffer

The IOBuffer is an abstraction that contains the data needed deal with
I/O like AsyncSocket and AsyncFile. The IOBuffer is very flexible since
it can be used by the NioEventloop (which requires java.nio.ByteBuffer)
but it can also be used by io_uring (which requires a pointer and
length).

The IOBuffer is the most critical part of the I/O design. For the 5.3
release it is simple (just wrapper around ByteBuffer), but a lot of
improvements are planned.

## Reference

https://hazelcast.atlassian.net/wiki/spaces/EN/pages/4441833496/TDD+of+the+thread-per-core+engine
