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

import com.hazelcast.internal.tpc.nio.NioReactorBuilder;

import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpc.util.Preconditions.checkPositive;

/**
 * Configuration options for the {@link TpcEngine}.
 */
public class Configuration {

    public static final String NAME_REACTOR_COUNT = "hazelcast.tpc.reactor.count";

    int reactorCount = Integer.getInteger(NAME_REACTOR_COUNT, Runtime.getRuntime().availableProcessors());

    ReactorBuilder reactorBuilder = new NioReactorBuilder();

    /**
     * Sets the ReactorBuilder.
     *
     * @param reactorBuilder the reactorBuilder.
     * @throws NullPointerException if reactorBuilder is null.
     */
    public void setReactorBuilder(ReactorBuilder reactorBuilder) {
        this.reactorBuilder = checkNotNull(reactorBuilder, "reactorBuilder");
    }

    /**
     * Sets the number of reactors.
     *
     * @param reactorCount the number of reactors.
     * @throws IllegalArgumentException if reactorCount smaller than 1.
     */
    public void setReactorCount(int reactorCount) {
        this.reactorCount = checkPositive(reactorCount, "reactorCount");
    }
}
