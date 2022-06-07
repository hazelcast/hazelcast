/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.tpc.engine;

import com.hazelcast.internal.util.ThreadAffinity;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.tpc.engine.epoll.EpollEventloop.EpollConfiguration;
import com.hazelcast.tpc.engine.iouring.IOUringAsyncSocket;
import com.hazelcast.tpc.engine.iouring.IOUringEventloop.IOUringConfiguration;
import com.hazelcast.tpc.engine.nio.NioAsyncSocket;
import com.hazelcast.tpc.engine.nio.NioEventloop;
import com.hazelcast.tpc.engine.epoll.EpollEventloop;
import com.hazelcast.tpc.engine.iouring.IOUringEventloop;
import com.hazelcast.tpc.engine.nio.NioEventloop.NioConfiguration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.tpc.engine.Engine.State.NEW;
import static com.hazelcast.tpc.engine.Engine.State.RUNNING;
import static com.hazelcast.tpc.engine.Engine.State.SHUTDOWN;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.getProperty;

/**
 * The Engine is effectively an array of eventloops
 *
 * The Engine is not aware of any specific applications. E.g. it could execute operations, but it
 * can equally well run client requests or completely different applications.
 */
public final class Engine {

    private final boolean monitorSilent;
    private final Eventloop.Type eventloopType;
    private final int eventloopCount;
    private final Eventloop[] eventloops;
    private final MonitorThread monitorThread;
    private final AtomicReference<State> state = new AtomicReference<>(NEW);
    final CountDownLatch terminationLatch;

    /**
     * Creates an Engine with the default {@link Configuration}.
     */
    public Engine() {
        this(new Configuration());
    }

    /**
     * Creates an Engine with the given Configuration.
     *
     * @param configuration the Configuration.
     * @throws NullPointerException when configuration is null.
     */
    public Engine(Configuration configuration) {
        this.eventloopCount = configuration.eventloopCount;
        this.eventloopType = configuration.eventloopType;
        this.monitorSilent = configuration.monitorSilent;
        this.eventloops = new Eventloop[eventloopCount];
        this.monitorThread = new MonitorThread(eventloops, monitorSilent);
        this.terminationLatch = new CountDownLatch(eventloopCount);

        for (int idx = 0; idx < eventloopCount; idx++) {
            switch (eventloopType) {
                case NIO:
                    NioConfiguration nioConfiguration = new NioConfiguration();
                    nioConfiguration.setThreadAffinity(configuration.threadAffinity);
                    nioConfiguration.setThreadName("eventloop-" + idx);
                    configuration.eventloopConfigUpdater.accept(nioConfiguration);
                    eventloops[idx] = new NioEventloop(nioConfiguration);
                    break;
                case EPOLL:
                    EpollConfiguration epollConfiguration = new EpollConfiguration();
                    epollConfiguration.setThreadAffinity(configuration.threadAffinity);
                    epollConfiguration.setThreadName("eventloop-" + idx);
                    configuration.eventloopConfigUpdater.accept(epollConfiguration);
                    eventloops[idx] = new EpollEventloop(epollConfiguration);
                    break;
                case IOURING:
                    IOUringConfiguration ioUringConfiguration = new IOUringConfiguration();
                    ioUringConfiguration.setThreadName("eventloop-" + idx);
                    ioUringConfiguration.setThreadAffinity(configuration.threadAffinity);
                    configuration.eventloopConfigUpdater.accept(ioUringConfiguration);
                    eventloops[idx] = new IOUringEventloop(ioUringConfiguration);
                    break;
                default:
                    throw new IllegalStateException("Unknown eventloopType:" + eventloopType);
            }
            eventloops[idx].engine = this;
        }
    }

    /**
     * Returns the Engine State.
     *
     * This method is thread-safe.
     *
     * @return the engine state.
     */
    public State state() {
        return state.get();
    }

    /**
     * Returns the type of Eventloop used by this Engine.
     *
     * @return the type of Eventloop.
     */
    public Eventloop.Type eventloopType() {
        return eventloopType;
    }

    /**
     * Returns the eventloops.
     *
     * @return the {@link Eventloop}s.
     */
    public Eventloop[] eventloops() {
        return eventloops;
    }

    /**
     * Returns the number of Eventloop instances in this Engine.
     *
     * This method is thread-safe.
     *
     * @return the number of eventloop instances.
     */
    public int eventloopCount() {
        return eventloopCount;
    }

    /**
     * Gets the {@link Eventloop} at the given index.
     *
     * @param idx the index of the Eventloop.
     * @return The Eventloop at the given index.
     */
    public Eventloop eventloop(int idx) {
        return eventloops[idx];
    }

    /**
     * Starts the Engine by starting all the {@link Eventloop} instances.
     *
     * @throws IllegalStateException if
     */
    public void start() {
        for (; ; ) {
            State oldState = state.get();
            if (oldState != NEW) {
                throw new IllegalStateException("Can't start Engine, it isn't in NEW state.");
            }

            if (!state.compareAndSet(oldState, RUNNING)) {
                continue;
            }

            for (Eventloop eventloop : eventloops) {
                eventloop.start();
            }

            monitorThread.start();
            return;
        }
    }

    /**
     * Shuts down the Engine. If the Engine is already shutdown or terminated, the call is ignored.
     *
     * This method is thread-safe.
     */
    public void shutdown() {
        for (; ; ) {
            State oldState = state.get();
            switch (oldState) {
                case NEW:
                    if (!state.compareAndSet(oldState, SHUTDOWN)) {
                        continue;
                    }
                    break;
                case RUNNING:
                    if (!state.compareAndSet(oldState, SHUTDOWN)) {
                        continue;
                    }
                    break;
                case SHUTDOWN:
                    return;
                case TERMINATED:
                    return;
                default:
                    throw new IllegalStateException();
            }

            monitorThread.shutdown();

            for (Eventloop eventloop : eventloops) {
                eventloop.shutdown();
            }
        }
    }

    /**
     * Awaits for the termination of the Engine.
     *
     * This method is thread-safe.
     *
     * @param timeout the timeout
     * @param unit    the TimeUnit
     * @return true if the Engine is terminated.
     * @throws InterruptedException if the calling thread got interrupted while waiting.
     */
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return terminationLatch.await(timeout, unit);
    }

    void notifyEventloopTerminated() {
        synchronized (terminationLatch){
            if(terminationLatch.getCount()==1){
                state.set(State.TERMINATED);
            }
            terminationLatch.countDown();
        }
    }

    /**
     * Contains the configuration of the {@link Engine}.
     */
    public static class Configuration {
        private int eventloopCount = Integer.parseInt(getProperty("reactor.count", "" + Runtime.getRuntime().availableProcessors()));
        private Eventloop.Type eventloopType = Eventloop.Type.fromString(getProperty("reactor.type", "nio"));
        private ThreadAffinity threadAffinity = ThreadAffinity.newSystemThreadAffinity("reactor.cpu-affinity");
        private boolean monitorSilent = Boolean.parseBoolean(getProperty("reactor.monitor.silent", "false"));
        private Consumer<Eventloop.Configuration> eventloopConfigUpdater = configuration -> {
        };

        public void setEventloopType(Eventloop.Type eventloopType) {
            this.eventloopType = checkNotNull(eventloopType, "eventloopType can't be null");
        }

        public void setEventloopConfigUpdater(Consumer<Eventloop.Configuration> eventloopConfigUpdater) {
            this.eventloopConfigUpdater = eventloopConfigUpdater;
        }

        public void setEventloopCount(int eventloopCount) {
            this.eventloopCount = checkPositive("reactorCount", eventloopCount);
        }

        public void setMonitorSilent(boolean monitorSilent) {
            this.monitorSilent = monitorSilent;
        }
    }

    public enum State {
        NEW,
        RUNNING,
        SHUTDOWN,
        TERMINATED
    }

    private static final class MonitorThread extends Thread {

        private final Eventloop[] eventloops;
        private final boolean silent;
        private volatile boolean shutdown = false;
        private long prevMillis = currentTimeMillis();
        // There is a memory leak on the counters. When channels die, counters are not removed.
        private final Map<SwCounter, LongHolder> prevMap = new HashMap<>();

        private MonitorThread(Eventloop[] eventloops, boolean silent) {
            super("MonitorThread");
            this.eventloops = eventloops;
            this.silent = silent;
        }

        static class LongHolder {
            public long value;
        }

        @Override
        public void run() {
            try {
                long nextWakeup = currentTimeMillis() + 5000;
                while (!shutdown) {
                    try {
                        long now = currentTimeMillis();
                        long remaining = nextWakeup - now;
                        if (remaining > 0) {
                            Thread.sleep(remaining);
                        }
                    } catch (InterruptedException e) {
                        return;
                    }

                    nextWakeup += 5000;

                    long currentMillis = currentTimeMillis();
                    long elapsed = currentMillis - prevMillis;
                    this.prevMillis = currentMillis;

                    for (Eventloop eventloop : eventloops) {
                        //                    for (AsyncSocket socket : eventloop.resources()) {
                        //                        monitor(socket, elapsed);
                        //                    }
                    }

                    if (!silent) {
                        for (Eventloop eventloop : eventloops) {
                            monitor(eventloop, elapsed);
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void monitor(Eventloop eventloop, long elapsed) {
            //log(reactor + " request-count:" + reactor.requests.get());
            //log(reactor + " channel-count:" + reactor.channels().size());

            //        long requests = reactor.requests.get();
            //        LongHolder prevRequests = getPrev(reactor.requests);
            //        long requestsDelta = requests - prevRequests.value;
            //log(reactor + " " + thp(requestsDelta, elapsed) + " requests/second");
            //prevRequests.value = requests;
            //
            //        log("head-block:" + reactor.reactorQueue.head_block
            //                + " tail:" + reactor.reactorQueue.tail.get()
            //                + " dirtyHead:" + reactor.reactorQueue.dirtyHead.get()
            //                + " cachedTail:" + reactor.reactorQueue.cachedTail);
            ////
        }

        private void log(String s) {
            System.out.println("[monitor] " + s);
        }

        private void monitor(AsyncSocket socket, long elapsed) {
            long packetsRead = socket.framesRead.get();
            LongHolder prevPacketsRead = getPrev(socket.framesRead);
            long packetsReadDelta = packetsRead - prevPacketsRead.value;

            if (!silent) {
                log(socket + " " + thp(packetsReadDelta, elapsed) + " packets/second");

                long bytesRead = socket.bytesRead.get();
                LongHolder prevBytesRead = getPrev(socket.bytesRead);
                long bytesReadDelta = bytesRead - prevBytesRead.value;
                log(socket + " " + thp(bytesReadDelta, elapsed) + " bytes-read/second");
                prevBytesRead.value = bytesRead;

                long bytesWritten = socket.bytesWritten.get();
                LongHolder prevBytesWritten = getPrev(socket.bytesWritten);
                long bytesWrittenDelta = bytesWritten - prevBytesWritten.value;
                log(socket + " " + thp(bytesWrittenDelta, elapsed) + " bytes-written/second");
                prevBytesWritten.value = bytesWritten;

                long handleOutboundCalls = socket.handleWriteCnt.get();
                LongHolder prevHandleOutboundCalls = getPrev(socket.handleWriteCnt);
                long handleOutboundCallsDelta = handleOutboundCalls - prevHandleOutboundCalls.value;
                log(socket + " " + thp(handleOutboundCallsDelta, elapsed) + " handleOutbound-calls/second");
                prevHandleOutboundCalls.value = handleOutboundCalls;

                long readEvents = socket.readEvents.get();
                LongHolder prevReadEvents = getPrev(socket.readEvents);
                long readEventsDelta = readEvents - prevReadEvents.value;
                log(socket + " " + thp(readEventsDelta, elapsed) + " read-events/second");
                prevReadEvents.value = readEvents;

                log(socket + " " + (packetsReadDelta * 1.0f / (handleOutboundCallsDelta + 1)) + " packets/handleOutbound-call");
                log(socket + " " + (packetsReadDelta * 1.0f / (readEventsDelta + 1)) + " packets/read-events");
                log(socket + " " + (bytesReadDelta * 1.0f / (readEventsDelta + 1)) + " bytes-read/read-events");
            }
            prevPacketsRead.value = packetsRead;

            if (packetsReadDelta == 0 || true) {
                if (socket instanceof NioAsyncSocket) {
                    NioAsyncSocket c = (NioAsyncSocket) socket;
                    boolean hasData = !c.unflushedFrames.isEmpty() || !c.ioVector.isEmpty();
                    //if (nioChannel.flushThread.get() == null && hasData) {
                    log(socket + " is stuck: unflushed-frames:" + c.unflushedFrames.size()
                            + " ioVector.empty:" + c.ioVector.isEmpty()
                            + " flushed:" + c.flushThread.get()
                            + " eventloop.contains:" + c.eventloop().concurrentRunQueue.contains(c));
                    //}
                } else if (socket instanceof IOUringAsyncSocket) {
                    IOUringAsyncSocket c = (IOUringAsyncSocket) socket;
                    boolean hasData = !c.unflushedFrames.isEmpty() || !c.ioVector.isEmpty();
                    //if (c.flushThread.get() == null && hasData) {
                    log(socket + " is stuck: unflushed-frames:" + c.unflushedFrames.size()
                            + " ioVector.empty:" + c.ioVector.isEmpty()
                            + " flushed:" + c.flushThread.get()
                            + " eventloop.contains:" + c.eventloop().concurrentRunQueue.contains(c));
                    //}
                }
            }
        }

        private LongHolder getPrev(SwCounter counter) {
            LongHolder prev = prevMap.get(counter);
            if (prev == null) {
                prev = new LongHolder();
                prevMap.put(counter, prev);
            }
            return prev;
        }

        private static float thp(long delta, long elapsed) {
            return (delta * 1000f) / elapsed;
        }

        private void shutdown() {
            shutdown = true;
            interrupt();
        }
    }
}
