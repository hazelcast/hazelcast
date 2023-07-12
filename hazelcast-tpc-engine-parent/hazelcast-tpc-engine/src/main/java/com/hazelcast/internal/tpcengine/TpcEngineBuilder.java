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

import com.hazelcast.internal.tpcengine.nio.NioReactorBuilder;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkPositive;

/**
 * The builder for the the {@link TpcEngine}.
 * <p>
 * todo: deal with affinity.
 */
public class TpcEngineBuilder {

    public static final String NAME_REACTOR_COUNT = "hazelcast.tpc.reactor.count";

    int reactorCount = Integer.getInteger(NAME_REACTOR_COUNT, Runtime.getRuntime().availableProcessors());

    Supplier<String> threadNameFn;

    Supplier<String> reactorNameFn = new Supplier<>() {
        private final AtomicInteger idGenerator = new AtomicInteger();

        @Override
        public String get() {
            return "Reactor-" + idGenerator.incrementAndGet();
        }
    };

    Supplier<ReactorBuilder> reactorBuilderFn = new Supplier<>() {
        @Override
        public ReactorBuilder get() {
            NioReactorBuilder reactorBuilder = new NioReactorBuilder();
            if (threadNameFn != null) {
                reactorBuilder.setThreadName(threadNameFn.get());
            }
            reactorBuilder.setReactorName(reactorNameFn.get());
            reactorBuilder.setThreadFactory(threadFactory);
            return reactorBuilder;
        }
    };

    private ThreadFactory threadFactory = Thread::new;

    /**
     * Sets the ThreadFactory to use for creating the Reactor threads.
     *
     * @param threadFactory the {@link ThreadFactory}.
     * @throws NullPointerException if threadFactory is null.
     */
    public void setThreadFactory(ThreadFactory threadFactory) {
        this.threadFactory = checkNotNull(threadFactory, "threadFactory");
    }

    /**
     * Sets the function that provides the thread name. If this function is not set,
     * then the thread provided by the threadFactory will be used.
     *
     * @param threadNameFn the function that provides the thread name.
     * @throws NullPointerException if threadNameFn is null.
     */
    public void setThreadNameFn(Supplier<String> threadNameFn) {
        this.threadNameFn = checkNotNull(threadNameFn, "threadNameFn");
    }

    /**
     * Sets the function that provides the reactor name. If this function isn't
     * provided, then a default implementation will be used.
     *
     * @param reactorNameFn the function that provides the reactor name.
     * @throws NullPointerException if reactorNameFn is null.
     */
    public void setReactorNameFn(Supplier<String> reactorNameFn) {
        this.reactorNameFn = checkNotNull(reactorNameFn, "reactorNameFn");
    }

    /**
     * Sets the function that provides a ReactorBuilder instance.
     *
     * @param reactorBuilderFn the reactorBuilderFn.
     * @return this
     * @throws NullPointerException if reactorBuilderFn is <code>null</code>.
     */
    public TpcEngineBuilder setReactorBuilderFn(Supplier<ReactorBuilder> reactorBuilderFn) {
        this.reactorBuilderFn = checkNotNull(reactorBuilderFn, "reactorBuilderFn");
        return this;
    }

    /**
     * Sets the number of reactors.
     *
     * @param reactorCount the number of reactors.
     * @return this
     * @throws IllegalArgumentException if reactorCount smaller than 1.
     */
    public TpcEngineBuilder setReactorCount(int reactorCount) {
        this.reactorCount = checkPositive(reactorCount, "reactorCount");
        return this;
    }

    /**
     * Builds a single TpcEngine instance.
     * <p/>
     * This method can be called multiple times although in practice is unlikely to happen.
     *
     * @return the created instance.
     */
    public TpcEngine build() {
        return new TpcEngine(this);
    }
}
