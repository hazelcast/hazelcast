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
 * This accumulator immediately publishes incoming events by only giving a sequence number to them.
 *
 * An instance of this class is called by at most one thread at a time.
 */
class NonStopPublisherAccumulator extends BasicAccumulator<Sequenced> {

    NonStopPublisherAccumulator(QueryCacheContext context, AccumulatorInfo info) {
        super(context, info);
    }

    @Override
    public void accumulate(Sequenced eventData) {
        super.accumulate(eventData);

        AccumulatorInfo info = getInfo();
        if (!info.isPublishable()) {
            return;
        }

        poll(handler, 0, TimeUnit.SECONDS);
    }
}
