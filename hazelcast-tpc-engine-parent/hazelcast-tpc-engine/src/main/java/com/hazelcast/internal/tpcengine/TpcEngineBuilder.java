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

import java.util.function.Supplier;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkPositive;

/**
 * The builder for the the {@link TpcEngine}. Can only be used once.
 */
public final class TpcEngineBuilder {

    public static final String NAME_REACTOR_COUNT = "hazelcast.tpc.reactor.count";

    int reactorCount = Integer.getInteger(NAME_REACTOR_COUNT, Runtime.getRuntime().availableProcessors());
    Supplier<ReactorBuilder> reactorBuilderFn = NioReactorBuilder::new;
    private boolean built;


    /**
     * Sets the function that provides a ReactorBuilder instance.
     *
     * @param reactorBuilderFn the reactorBuilderFn.
     * @return this
     * @throws NullPointerException  if <code>reactorBuilderFn</code> is <code>null</code>.
     * @throws IllegalStateException if a TpcEngine already has already been built.
     */
    public TpcEngineBuilder setReactorBuilderFn(Supplier<ReactorBuilder> reactorBuilderFn) {
        verifyNotBuilt();

        this.reactorBuilderFn = checkNotNull(reactorBuilderFn, "reactorBuilderFn");
        return this;
    }

    /**
     * Sets the number of reactors.
     *
     * @param reactorCount the number of reactors.
     * @return this
     * @throws IllegalArgumentException if <code>reactorCount</code> is smaller than 1.
     * @throws IllegalStateException    if a TpcEngine already has already been built.
     */
    public TpcEngineBuilder setReactorCount(int reactorCount) {
        verifyNotBuilt();

        this.reactorCount = checkPositive(reactorCount, "reactorCount");
        return this;
    }

    /**
     * Builds a single TpcEngine instance.
     *
     * @return the created instance.
     * @throws IllegalStateException if a TpcEngine already has already been built.
     */
    public TpcEngine build() {
        verifyNotBuilt();

        built = true;
        return new TpcEngine(this);
    }

    private void verifyNotBuilt() {
        if (built) {
            throw new IllegalStateException("Can't call build twice on the same ReactorBuilder");
        }
    }
}
