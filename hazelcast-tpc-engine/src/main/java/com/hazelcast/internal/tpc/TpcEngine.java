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

import com.hazelcast.internal.tpc.logging.TpcLogger;
import com.hazelcast.internal.tpc.logging.TpcLoggerLocator;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.tpc.TpcEngine.State.NEW;
import static com.hazelcast.internal.tpc.TpcEngine.State.RUNNING;
import static com.hazelcast.internal.tpc.TpcEngine.State.SHUTDOWN;
import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;

/**
 * The TpcEngine is effectively an array of reactors.
 * <p>
 * The TpcEngine is not aware of any specific applications. E.g. it could execute operations, but it
 * can equally well run client requests or completely different applications.
 */
public final class TpcEngine {

    final CountDownLatch terminationLatch;
    private final TpcLogger logger = TpcLoggerLocator.getLogger(TpcEngine.class);
    private final int reactorCount;
    private final Reactor[] reactors;
    private final AtomicReference<State> state = new AtomicReference<>(NEW);
    private final Configuration configuration;

    /**
     * Creates an TpcEngine with the default {@link Configuration}.
     */
    public TpcEngine() {
        this(new Configuration());
    }

    /**
     * Creates an TpcEngine with the given Configuration.
     *
     * @param configuration the Configuration for the TpcEngine.
     * @throws NullPointerException when configuration is null.
     */
    public TpcEngine(Configuration configuration) {
        this.configuration = checkNotNull(configuration, "configuration");
        this.reactorCount = configuration.reactorCount;
        this.reactors = new Reactor[reactorCount];
        this.terminationLatch = new CountDownLatch(reactorCount);

        ReactorBuilder reactorBuilder = configuration.reactorBuilder;
        reactorBuilder.engine = this;
        for (int idx = 0; idx < reactorCount; idx++) {
            reactors[idx] = reactorBuilder.build();
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
     * Returns the type of Reactor used by this TpcEngine.
     *
     * @return the type of Reactor.
     */
    public ReactorType reactorType() {
        return configuration.reactorBuilder.type;
    }

    /**
     * Returns the reactors.
     *
     * @return the {@link Reactor}s.
     */
    public Reactor[] reactors() {
        return reactors;
    }

    /**
     * Returns the number of reactor instances in this TpcEngine.
     * <p>
     * This method is thread-safe.
     *
     * @return the number of reactor instances.
     */
    public int reactorCount() {
        return reactorCount;
    }

    /**
     * Gets the {@link Reactor} at the given index.
     *
     * @param idx the index of the reactor.
     * @return The reactor at the given index.
     */
    public Reactor reactor(int idx) {
        return reactors[idx];
    }

    /**
     * Starts the TpcEngine by starting all the {@link Reactor} instances.
     *
     * @throws IllegalStateException if
     */
    public void start() {
        logger.info("Starting " + reactorCount + " reactors of type [" + reactorType() + "]");

        for (; ; ) {
            State oldState = state.get();
            if (oldState != NEW) {
                throw new IllegalStateException("Can't start TpcEngine, it isn't in NEW state.");
            }

            if (state.compareAndSet(oldState, RUNNING)) {
                break;
            }
        }

        for (Reactor reactor : reactors) {
            reactor.start();
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

            for (Reactor reactor : reactors) {
                reactor.shutdown();
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

    void notifyReactorTerminated() {
        synchronized (terminationLatch) {
            if (terminationLatch.getCount() == 1) {
                state.set(State.TERMINATED);
            }
            terminationLatch.countDown();
        }
    }

    public enum State {
        NEW,
        RUNNING,
        SHUTDOWN,
        TERMINATED
    }
}
