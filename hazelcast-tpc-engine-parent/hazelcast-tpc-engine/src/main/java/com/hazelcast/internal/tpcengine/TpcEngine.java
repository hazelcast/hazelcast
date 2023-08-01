/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine;

import com.hazelcast.internal.tpcengine.logging.TpcLogger;
import com.hazelcast.internal.tpcengine.logging.TpcLoggerLocator;
import com.hazelcast.internal.tpcengine.util.AbstractBuilder;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.hazelcast.internal.tpcengine.TpcEngine.State.NEW;
import static com.hazelcast.internal.tpcengine.TpcEngine.State.RUNNING;
import static com.hazelcast.internal.tpcengine.TpcEngine.State.SHUTDOWN;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkPositive;

/**
 * The TpcEngine is effectively an array of reactors.
 * <p/>
 * The TpcEngine is not aware of any specific applications. E.g. it could execute
 * operations, but it can equally well run client requests or completely different
 * applications.
 */
public final class TpcEngine {

    final CountDownLatch terminationLatch;
    private final TpcLogger logger = TpcLoggerLocator.getLogger(TpcEngine.class);
    private final int reactorCount;
    private final Reactor[] reactors;
    private final AtomicReference<State> state = new AtomicReference<>(NEW);
    private final ReactorType reactorType;

    /**
     * Creates an TpcEngine from the given Context.
     *
     * @param engineBuilder the context.
     * @throws NullPointerException when engineBuilder is <code>null</code>.
     */
    private TpcEngine(Builder engineBuilder) {
        this.reactorCount = engineBuilder.reactorCount;
        this.reactors = new Reactor[reactorCount];
        this.terminationLatch = new CountDownLatch(reactorCount);
        this.reactorType = engineBuilder.reactorType;
        for (int reactorIndex = 0; reactorIndex < reactorCount; reactorIndex++) {
            Reactor.Builder reactorBuilder = Reactor.Builder.newReactorBuilder(reactorType);
            engineBuilder.reactorConfigureFn.accept(reactorBuilder);
            reactorBuilder.engine = this;
            reactors[reactorIndex] = reactorBuilder.build();
        }
    }

    /**
     * Returns the TpcEngine State.
     * <p/>
     * This method is thread-safe.
     *
     * @return the engine state.
     */
    public State state() {
        return state.get();
    }

    /**
     * Returns the type of {@link Reactor} used by this TpcEngine.
     * <p/>
     * This method is thread-safe.
     *
     * @return the type of Reactor.
     */
    public ReactorType reactorType() {
        return reactorType;
    }

    /**
     * Returns the number of {@link Reactor} instances in this TpcEngine.
     * <p/>
     * This method is thread-safe.
     *
     * @return the number of reactor instances.
     */
    public int reactorCount() {
        return reactorCount;
    }

    /**
     * Gets the {@link Reactor} at the given index.
     * <p/>
     * This method is thread-safe.
     *
     * @param reactorIndex the index of the reactor.
     * @return The reactor at the given index.
     */
    public Reactor reactor(int reactorIndex) {
        return reactors[reactorIndex];
    }

    /**
     * Starts the TpcEngine by starting all the {@link Reactor} instances.
     * <p/>
     * This method is thread-safe.
     *
     * @throws IllegalStateException if the Reactor is shutdown or terminated.
     */
    public TpcEngine start() {
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

        return this;
    }

    /**
     * Shuts down the TpcEngine. If the TpcEngine is already shutdown or terminated,
     * the call is ignored.
     * <p/>
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
     * <p/>
     * This method is thread-safe.
     *
     * @param timeout the timeout. If the timeout is 0, then this call will not wait.
     * @param unit    the TimeUnit
     * @return <code>true</code> if the TpcEngine is terminated.
     * @throws InterruptedException if the calling thread got interrupted while waiting.
     * @throws NullPointerException if unit is <code>null</code>.
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

    /**
     * The Builder for the the {@link TpcEngine}.
     */
    @SuppressWarnings({"checkstyle:VisibilityModifier"})
    public static final class Builder extends AbstractBuilder<TpcEngine> {

        public static final String NAME_REACTOR_COUNT = "hazelcast.tpc.reactor.count";
        public static final String NAME_REACTOR_TYPE = "hazelcast.tpc.reactor.type";

        public int reactorCount = Integer.getInteger(NAME_REACTOR_COUNT, Runtime.getRuntime().availableProcessors());
        public Consumer<Reactor.Builder> reactorConfigureFn = builder -> {
        };

        public ReactorType reactorType = ReactorType.fromString(System.getProperty(NAME_REACTOR_TYPE, "nio"));

        @Override
        protected void conclude() {
            super.conclude();

            checkPositive(reactorCount, "reactorCount");
            checkNotNull(reactorType, "reactorType");
            checkNotNull(reactorConfigureFn, "reactorConfigureFn");
        }

        /**
         * Builds a single TpcEngine instance.
         *
         * @return the created instance.
         * @throws IllegalStateException if a TpcEngine already has already been built.
         */
        protected TpcEngine doBuild() {
            return new TpcEngine(this);
        }
    }
}
