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

import com.hazelcast.internal.tpc.nio.NioEventloopBuilder;

import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpc.util.Preconditions.checkPositive;

/**
 * Configuration options for the {@link TpcEngine}.
 */
public class Configuration {

    public static final String NAME_EVENTLOOP_COUNT = "hazelcast.tpc.eventloop.count";

    int eventloopCount = Integer.getInteger(NAME_EVENTLOOP_COUNT, Runtime.getRuntime().availableProcessors());

    EventloopBuilder eventloopBuilder = new NioEventloopBuilder();

    /**
     * Sets the EventloopBuilder.
     *
     * @param eventloopBuilder the eventloopBuilder.
     * @throws NullPointerException if eventloopBuilder is 0.
     */
    public void setEventloopBuilder(EventloopBuilder eventloopBuilder) {
        this.eventloopBuilder = checkNotNull(eventloopBuilder, "eventloopBuilder");
    }

    /**
     * Sets the number of eventloops.
     *
     * @param eventloopCount the number of eventloops.
     * @throws IllegalArgumentException if eventloopCount smaller than 1.
     */
    public void setEventloopCount(int eventloopCount) {
        this.eventloopCount = checkPositive(eventloopCount, "eventloopCount");
    }
}
