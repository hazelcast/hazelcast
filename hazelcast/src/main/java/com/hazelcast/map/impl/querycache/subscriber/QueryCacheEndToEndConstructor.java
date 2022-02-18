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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.internal.util.ConstructorFunction;

/**
 * Constructor which is used to construct all parts/sides of a {@code QueryCache} system on nodes or on clients.
 * This constructor is called from subscriber-side and initiates the construction of whole underlying system
 * which feeds the {@code QueryCache}.
 * <p>
 * When this constructor returns successfully, there should be a {@code QueryCache} instance ready to use.
 *
 * @see com.hazelcast.map.QueryCache
 */
public interface QueryCacheEndToEndConstructor extends ConstructorFunction<String, InternalQueryCache> {

    /**
     * Creates a new {@link SubscriberAccumulator}
     *
     * @param info {@link AccumulatorInfo} for the {@link SubscriberAccumulator}
     * @throws Exception in case of any exceptional case.
     */
    void createSubscriberAccumulator(AccumulatorInfo info) throws Exception;

    /**
     * Creates a new publisher-accumulator according to {@link AccumulatorInfo}.
     * This accumulator will be used to feed the {@link SubscriberAccumulator}.
     *
     * @param info {@link AccumulatorInfo} for the publisher-accumulator
     * @param urgent if create task is urgent(can be send even when client disconnected state)
     * @throws Exception in case of any exceptional case.
     * @see com.hazelcast.map.impl.querycache.publisher.NonStopPublisherAccumulator
     * @see com.hazelcast.map.impl.querycache.publisher.BatchPublisherAccumulator
     * @see com.hazelcast.map.impl.querycache.publisher.CoalescingPublisherAccumulator
     */
    void createPublisherAccumulator(AccumulatorInfo info, boolean urgent) throws Exception;
}
