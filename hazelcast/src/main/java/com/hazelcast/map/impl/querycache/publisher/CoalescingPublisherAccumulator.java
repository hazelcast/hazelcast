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
import com.hazelcast.map.impl.querycache.QueryCacheEventService;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorProcessor;
import com.hazelcast.map.impl.querycache.accumulator.BasicAccumulator;
import com.hazelcast.map.impl.querycache.event.BatchEventData;
import com.hazelcast.map.impl.querycache.event.QueryCacheEventData;
import com.hazelcast.map.impl.querycache.event.sequence.Sequenced;
import com.hazelcast.internal.serialization.Data;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * An {@link com.hazelcast.map.impl.querycache.accumulator.Accumulator} which coalesces keys during accumulation.
 */
class CoalescingPublisherAccumulator extends BasicAccumulator<QueryCacheEventData> {

    /**
     * Index map to hold last unpublished event sequence per key.
     */
    private final Map<Data, Long> index = new HashMap<>();

    CoalescingPublisherAccumulator(QueryCacheContext context, AccumulatorInfo info) {
        super(context, info);
    }

    @Override
    public void accumulate(QueryCacheEventData eventData) {
        setSequence(eventData);
        getBuffer().add(eventData);

        AccumulatorInfo info = getInfo();
        if (!info.isPublishable()) {
            return;
        }

        poll(handler, info.getBatchSize());
        poll(handler, info.getDelaySeconds(), SECONDS);
    }

    @Override
    public void reset() {
        index.clear();
        super.reset();
    }

    private void setSequence(QueryCacheEventData eventData) {
        Data dataKey = eventData.getDataKey();
        Long sequence = index.get(dataKey);
        if (sequence != null) {
            eventData.setSequence(sequence);
        } else {
            long nextSequence = partitionSequencer.nextSequence();
            eventData.setSequence(nextSequence);
            index.put(dataKey, nextSequence);
        }

        if (logger.isFinestEnabled()) {
            logger.finest(format("Added to index key=%s, sequence=%d, indexSize=%d",
                    eventData.getKey(), eventData.getSequence(), index.size()));
        }
    }

    @Override
    protected AccumulatorProcessor<Sequenced> createAccumulatorProcessor(AccumulatorInfo info,
                                                                         QueryCacheEventService eventService) {
        return new CoalescedEventAccumulatorProcessor(info, eventService);
    }

    /**
     * {@link EventPublisherAccumulatorProcessor} which additionally clears {@link #index} upon publishing.
     */
    private class CoalescedEventAccumulatorProcessor extends EventPublisherAccumulatorProcessor {

        CoalescedEventAccumulatorProcessor(AccumulatorInfo info, QueryCacheEventService eventService) {
            super(info, eventService);
        }

        @Override
        public void process(Sequenced sequenced) {
            super.process(sequenced);

            clearIndexes(sequenced);
        }

        private void clearIndexes(Sequenced sequenced) {
            if (sequenced instanceof BatchEventData) {
                Collection<QueryCacheEventData> events = ((BatchEventData) sequenced).getEvents();
                for (QueryCacheEventData event : events) {
                    removeFromIndex(event);
                }
                return;
            }

            if (sequenced instanceof QueryCacheEventData) {
                removeFromIndex((QueryCacheEventData) sequenced);
                return;
            }

            throw new IllegalArgumentException(format("Expected an instance of %s but found %s",
                    QueryCacheEventData.class.getSimpleName(), sequenced.getClass().getSimpleName()));
        }

        private void removeFromIndex(QueryCacheEventData eventData) {
            Data dataKey = eventData.getDataKey();
            index.remove(dataKey);

            if (logger.isFinestEnabled()) {
                logger.finest(format("Removed from index key=%s, sequence=%d, indexSize=%d",
                        eventData.getKey(), eventData.getSequence(), index.size()));
            }
        }
    }
}
