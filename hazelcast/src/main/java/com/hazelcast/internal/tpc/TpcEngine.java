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

package com.hazelcast.internal.tpc;

import com.hazelcast.internal.util.ThreadAffinity;
import com.hazelcast.internal.util.executor.HazelcastManagedThread;
import com.hazelcast.internal.tpc.nio.NioEventloop;
import com.hazelcast.internal.tpc.nio.NioEventloop.NioConfiguration;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.internal.tpc.TpcEngine.State.NEW;
import static com.hazelcast.internal.tpc.TpcEngine.State.RUNNING;
import static com.hazelcast.internal.tpc.TpcEngine.State.SHUTDOWN;
import static java.lang.System.getProperty;

/**
 * The TpcEngine is effectively an array of eventloops
 * <p>
 * The TpcEngine is not aware of any specific applications. E.g. it could execute operations, but it
 * can equally well run client requests or completely different applications.
 */
public final class TpcEngine {

    private final ILogger logger = Logger.getLogger(getClass());
    private final Eventloop.Type eventloopType;
    private final int eventloopCount;
    private final Eventloop[] eventloops;
    private final AtomicReference<State> state = new AtomicReference<>(NEW);
    final CountDownLatch terminationLatch;

    /**
     * Creates an TpcEngine with the default {@link Configuration}.
     */
    public TpcEngine() {
        this(new Configuration());
    }

    /**
     * Creates an TpcEngine with the given Configuration.
     *
     * @param cfg the Configuration.
     * @throws NullPointerException when configuration is null.
     */
    public TpcEngine(Configuration cfg) {
        this.eventloopCount = cfg.eventloopCount;
        this.eventloopType = cfg.eventloopType;
        this.eventloops = new Eventloop[eventloopCount];
        this.terminationLatch = new CountDownLatch(eventloopCount);

        for (int idx = 0; idx < eventloopCount; idx++) {
            switch (eventloopType) {
                case NIO:
                    NioConfiguration nioCfg = new NioConfiguration();
                    nioCfg.setThreadAffinity(cfg.threadAffinity);
                    nioCfg.setThreadName("eventloop-" + idx);
                    nioCfg.setThreadFactory(cfg.threadFactory);
                    cfg.eventloopConfigUpdater.accept(nioCfg);
                    eventloops[idx] = new NioEventloop(nioCfg);
                    break;
                default:
                    throw new IllegalStateException("Unknown eventloopType:" + eventloopType);
            }
            eventloops[idx].engine = this;
        }
    }

    /**
     * Returns the TpcEngine State.
     * <p>
     * This method is thread-safe.
     *
     * @return the engine state.
     */
    public State state() {
        return state.get();
    }

    /**
     * Returns the type of Eventloop used by this TpcEngine.
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
     * Returns the number of Eventloop instances in this TpcEngine.
     * <p>
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
     * Starts the TpcEngine by starting all the {@link Eventloop} instances.
     *
     * @throws IllegalStateException if
     */
    public void start() {
        logger.info("Starting " + eventloopCount + " eventloops");

        for (; ; ) {
            State oldState = state.get();
            if (oldState != NEW) {
                throw new IllegalStateException("Can't start TpcEngine, it isn't in NEW state.");
            }

            if (!state.compareAndSet(oldState, RUNNING)) {
                continue;
            }

            for (Eventloop eventloop : eventloops) {
                eventloop.start();
            }

            return;
        }
    }

    /**
     * Shuts down the TpcEngine. If the TpcEngine is already shutdown or terminated, the call is ignored.
     * <p>
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

            for (Eventloop eventloop : eventloops) {
                eventloop.shutdown();
            }
        }
    }

    /**
     * Awaits for the termination of the TpcEngine.
     * <p>
     * This method is thread-safe.
     *
     * @param timeout the timeout
     * @param unit    the TimeUnit
     * @return true if the TpcEngine is terminated.
     * @throws InterruptedException if the calling thread got interrupted while waiting.
     */
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return terminationLatch.await(timeout, unit);
    }

    void notifyEventloopTerminated() {
        synchronized (terminationLatch) {
            if (terminationLatch.getCount() == 1) {
                state.set(State.TERMINATED);
            }
            terminationLatch.countDown();
        }
    }

    /**
     * Contains the configuration of the {@link TpcEngine}.
     */
    public static class Configuration {
        private int eventloopCount = Integer.parseInt(getProperty("reactor.count", "" + Runtime.getRuntime().availableProcessors()));
        private Eventloop.Type eventloopType = Eventloop.Type.fromString(getProperty("reactor.type", "nio"));
        private ThreadAffinity threadAffinity = ThreadAffinity.newSystemThreadAffinity("reactor.cpu-affinity");
        private boolean monitorSilent = Boolean.parseBoolean(getProperty("reactor.monitor.silent", "false"));
        private ThreadFactory threadFactory = HazelcastManagedThread::new;
        private Consumer<Eventloop.Configuration> eventloopConfigUpdater = configuration -> {
        };

        public void setThreadAffinity(ThreadAffinity threadAffinity) {
            this.threadAffinity = threadAffinity;
        }

        public void setThreadFactory(ThreadFactory threadFactory) {
            this.threadFactory = checkNotNull(threadFactory, "threadFactory can't be null");
        }

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

}
