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
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.accumulator.BasicAccumulator;
import com.hazelcast.map.impl.querycache.event.sequence.Sequenced;

import java.util.concurrent.TimeUnit;

/**
 * When batching is enabled by setting {@link com.hazelcast.config.QueryCacheConfig#batchSize} to a value
 * greater than {@code 1}, this {@code Accumulator} is used to collect events.
 *
 * This {@code Accumulator} uses batch publishing logic in the {@link EventPublisherAccumulatorProcessor} class.
 *
 * @see EventPublisherAccumulatorProcessor
 */
class BatchPublisherAccumulator extends BasicAccumulator<Sequenced> {

    BatchPublisherAccumulator(QueryCacheContext context, AccumulatorInfo info) {
        super(context, info);
    }

    @Override
    public void accumulate(Sequenced sequenced) {
        super.accumulate(sequenced);

        AccumulatorInfo info = getInfo();
        if (!info.isPublishable()) {
            return;
        }

        poll(handler, info.getBatchSize());
        poll(handler, info.getDelaySeconds(), TimeUnit.SECONDS);
    }
}
