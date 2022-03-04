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
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfoSupplier;
import com.hazelcast.map.impl.querycache.event.QueryCacheEventData;
import com.hazelcast.map.impl.querycache.event.QueryCacheEventDataBuilder;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Responsible for sweeping all registered accumulators of a {@link PublisherContext}.
 * This is needed in situations like ownership changes/graceful shutdown.
 */
public final class AccumulatorSweeper {

    public static final long END_SEQUENCE = -1;

    private AccumulatorSweeper() {
    }

    public static void flushAllAccumulators(PublisherContext publisherContext) {
        QueryCacheContext context = publisherContext.getContext();
        EventPublisherAccumulatorProcessor processor
                = new EventPublisherAccumulatorProcessor(context.getQueryCacheEventService());
        PublisherAccumulatorHandler handler = new PublisherAccumulatorHandler(context, processor);

        MapPublisherRegistry mapPublisherRegistry = publisherContext.getMapPublisherRegistry();
        Map<String, PublisherRegistry> allPublisherRegistryMap = mapPublisherRegistry.getAll();

        for (PublisherRegistry publisherRegistry : allPublisherRegistryMap.values()) {
            Map<String, PartitionAccumulatorRegistry> accumulatorRegistryMap = publisherRegistry.getAll();
            for (PartitionAccumulatorRegistry accumulatorRegistry : accumulatorRegistryMap.values()) {
                Map<Integer, Accumulator> accumulatorMap = accumulatorRegistry.getAll();
                for (Map.Entry<Integer, Accumulator> entry : accumulatorMap.entrySet()) {
                    Integer partitionId = entry.getKey();
                    Accumulator accumulator = entry.getValue();

                    processor.setInfo(accumulator.getInfo());

                    // give 0 to delay-time in order to fetch all events in the accumulator
                    accumulator.poll(handler, 0, TimeUnit.SECONDS);

                    // send end event
                    QueryCacheEventData eventData = createEndOfSequenceEvent(partitionId);
                    processor.process(eventData);
                }
            }
        }
    }

    public static void flushAccumulator(PublisherContext publisherContext, int partitionId) {
        QueryCacheEventData endOfSequenceEvent = createEndOfSequenceEvent(partitionId);

        QueryCacheContext context = publisherContext.getContext();
        EventPublisherAccumulatorProcessor processor
                = new EventPublisherAccumulatorProcessor(context.getQueryCacheEventService());
        PublisherAccumulatorHandler handler = new PublisherAccumulatorHandler(context, processor);

        MapPublisherRegistry mapPublisherRegistry = publisherContext.getMapPublisherRegistry();
        Map<String, PublisherRegistry> allPublisherRegistryMap = mapPublisherRegistry.getAll();

        for (PublisherRegistry publisherRegistry : allPublisherRegistryMap.values()) {
            Map<String, PartitionAccumulatorRegistry> accumulatorRegistryMap = publisherRegistry.getAll();
            for (PartitionAccumulatorRegistry accumulatorRegistry : accumulatorRegistryMap.values()) {
                Map<Integer, Accumulator> accumulatorMap = accumulatorRegistry.getAll();
                Accumulator accumulator = accumulatorMap.get(partitionId);
                if (accumulator == null) {
                    continue;
                }

                processor.setInfo(accumulator.getInfo());

                // give 0 to delay-time in order to fetch all events in the accumulator
                accumulator.poll(handler, 0, TimeUnit.SECONDS);
                // send end event
                processor.process(endOfSequenceEvent);
            }
        }
    }

    public static void sendEndOfSequenceEvents(PublisherContext publisherContext, int partitionId) {
        QueryCacheEventData endOfSequenceEvent = createEndOfSequenceEvent(partitionId);

        QueryCacheContext context = publisherContext.getContext();
        EventPublisherAccumulatorProcessor processor
                = new EventPublisherAccumulatorProcessor(context.getQueryCacheEventService());

        AccumulatorInfoSupplier infoSupplier = publisherContext.getAccumulatorInfoSupplier();
        ConcurrentMap<String, ConcurrentMap<String, AccumulatorInfo>> all = infoSupplier.getAll();
        for (ConcurrentMap<String, AccumulatorInfo> oneMapsAccumulators : all.values()) {
            for (AccumulatorInfo accumulatorInfo : oneMapsAccumulators.values()) {
                if (accumulatorInfo.getDelaySeconds() == 0) {
                    processor.setInfo(accumulatorInfo);
                    processor.process(endOfSequenceEvent);
                }
            }
        }
    }

    public static void removeAccumulator(PublisherContext publisherContext, int partitionId) {
        MapPublisherRegistry mapPublisherRegistry = publisherContext.getMapPublisherRegistry();
        Map<String, PublisherRegistry> allPublisherRegistryMap = mapPublisherRegistry.getAll();

        for (PublisherRegistry publisherRegistry : allPublisherRegistryMap.values()) {
            Map<String, PartitionAccumulatorRegistry> accumulatorRegistryMap = publisherRegistry.getAll();
            for (PartitionAccumulatorRegistry accumulatorRegistry : accumulatorRegistryMap.values()) {
                accumulatorRegistry.remove(partitionId);
            }
        }
    }

    /**
     * In graceful shutdown, we are flushing all unsent events in an {@code Accumulator}. This event
     * will be the last event of an {@code Accumulator} upon flush and it is used to inform subscriber-side
     * to state that we reached the end of event sequence for the relevant partition.
     * <p>
     * After this event received by subscriber-side, subscriber resets its next-expected-sequence counter to zero for the
     * corresponding partition.
     */
    private static QueryCacheEventData createEndOfSequenceEvent(int partitionId) {
        return QueryCacheEventDataBuilder.newQueryCacheEventDataBuilder(false)
                .withSequence(END_SEQUENCE).withPartitionId(partitionId).build();
    }
}
