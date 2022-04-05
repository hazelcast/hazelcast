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

package com.hazelcast.map.impl.querycache.publisher;

import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.accumulator.Accumulator;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorFactory;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;

/**
 * Factory which is responsible for creating {@link Accumulator} implementations.
 * according to supplied {@link AccumulatorInfo}.
 *
 * @see Accumulator
 */
public class PublisherAccumulatorFactory implements AccumulatorFactory {

    private final QueryCacheContext context;

    public PublisherAccumulatorFactory(QueryCacheContext context) {
        this.context = context;
    }

    @Override
    public Accumulator createAccumulator(AccumulatorInfo info) {
        long delayTime = info.getDelaySeconds();

        if (delayTime <= 0L) {
            return new NonStopPublisherAccumulator(context, info);
        } else {
            if (info.isCoalesce()) {
                return new CoalescingPublisherAccumulator(context, info);
            } else {
                return new BatchPublisherAccumulator(context, info);
            }
        }
    }
}
