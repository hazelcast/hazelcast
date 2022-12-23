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

import com.hazelcast.internal.tpc.logging.TpcLoggerLocator;
import com.hazelcast.internal.tpc.logging.TpcLogger;
import com.hazelcast.internal.tpc.nio.NioEventloop.NioConfiguration;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.tpc.TpcEngine.State.NEW;
import static com.hazelcast.internal.tpc.TpcEngine.State.RUNNING;
import static com.hazelcast.internal.tpc.TpcEngine.State.SHUTDOWN;
import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpc.util.Preconditions.checkPositive;
import static java.lang.System.getProperty;

/**
 * The TpcEngine is effectively an array of eventloops
 * <p>
 * The TpcEngine is not aware of any specific applications. E.g. it could execute operations, but it
 * can equally well run client requests or completely different applications.
 */
public final class TpcEngine {

    final CountDownLatch terminationLatch;
    private final TpcLogger logger = TpcLoggerLocator.getLogger(TpcEngine.class);
    private final int eventloopCount;
    private final Eventloop[] eventloops;
    private final AtomicReference<State> state = new AtomicReference<>(NEW);
    private final Configuration engineCfg;

    /**
     * Creates an TpcEngine with the default {@link Configuration}.
     */
    public TpcEngine() {
        this(new Configuration());
    }

    /**
     * Creates an TpcEngine with the given Configuration.
     *
     * @param engineCfg the Configuration for the TpcEngine.
     * @throws NullPointerException when configuration is null.
     */
    public TpcEngine(Configuration engineCfg) {
        this.engineCfg = engineCfg;
        this.eventloopCount = engineCfg.eventloopCount;
        Eventloop.Configuration eventloopCfg = engineCfg.eventloopConfiguration;
        this.eventloops = new Eventloop[eventloopCount];
        this.terminationLatch = new CountDownLatch(eventloopCount);

        for (int idx = 0; idx < eventloopCount; idx++) {
            eventloops[idx] = eventloopCfg.create();
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
        return engineCfg.eventloopConfiguration.type;
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
        logger.info("Starting " + eventloopCount + " eventloops of type [" + eventloopType() + "]");

        for (; ; ) {
            State oldState = state.get();
            if (oldState != NEW) {
                throw new IllegalStateException("Can't start TpcEngine, it isn't in NEW state.");
            }

            if (state.compareAndSet(oldState, RUNNING)) {
                break;
            }
        }

        for (Eventloop eventloop : eventloops) {
            eventloop.start();
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
                case RUNNING:
                    if (!state.compareAndSet(oldState, SHUTDOWN)) {
                        continue;
                    }
                    break;
                case SHUTDOWN:
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
        private int eventloopCount = Integer.parseInt(
                getProperty("hazelcast.tpc.eventloop.count", "" + Runtime.getRuntime().availableProcessors()));

        private Eventloop.Configuration eventloopConfiguration = new NioConfiguration();

        public void setEventloopConfiguration(Eventloop.Configuration eventloopConfiguration) {
            this.eventloopConfiguration = checkNotNull(eventloopConfiguration, "eventloopConfiguration");
        }

        public void setEventloopCount(int eventloopCount) {
            this.eventloopCount = checkPositive("eventloopCount", eventloopCount);
        }
    }

    public enum State {
        NEW,
        RUNNING,
        SHUTDOWN,
        TERMINATED
    }

}
