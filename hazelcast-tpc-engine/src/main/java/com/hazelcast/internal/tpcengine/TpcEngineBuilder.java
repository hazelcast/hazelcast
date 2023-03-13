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

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkPositive;

/**
 * The builder for the the {@link TpcEngine}.
 */
public class TpcEngineBuilder {

    public static final String NAME_REACTOR_COUNT = "hazelcast.tpc.reactor.count";

    int reactorCount = Integer.getInteger(NAME_REACTOR_COUNT, Runtime.getRuntime().availableProcessors());

    ReactorBuilder reactorBuilder = new NioReactorBuilder();

    /**
     * Sets the ReactorBuilder.
     *
     * @param reactorBuilder the reactorBuilder.
     * @return this
     * @throws NullPointerException if reactorBuilder is <code>null</code>.
     */
    public TpcEngineBuilder setReactorBuilder(ReactorBuilder reactorBuilder) {
        this.reactorBuilder = checkNotNull(reactorBuilder, "reactorBuilder");
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
