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
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorHandler;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorProcessor;
import com.hazelcast.map.impl.querycache.event.BatchEventData;
import com.hazelcast.map.impl.querycache.event.QueryCacheEventData;
import com.hazelcast.map.impl.querycache.event.sequence.Sequenced;
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.properties.ClusterProperty;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static com.hazelcast.internal.util.MapUtil.createHashMap;

/**
 * Handler for processing of event data in an {@link Accumulator Accumulator}.
 * Processing is done by the help of {@link AccumulatorProcessor}.
 *
 * @see EventPublisherAccumulatorProcessor
 */
public class PublisherAccumulatorHandler implements AccumulatorHandler<Sequenced> {

    private final QueryCacheContext context;
    private final AccumulatorProcessor<Sequenced> processor;

    private Queue<QueryCacheEventData> eventCollection;

    public PublisherAccumulatorHandler(QueryCacheContext context, AccumulatorProcessor<Sequenced> processor) {
        this.context = context;
        this.processor = processor;
    }

    @Override
    public void handle(Sequenced eventData, boolean lastElement) {
        if (eventCollection == null) {
            eventCollection = new ArrayDeque<>();
        }

        eventCollection.add((QueryCacheEventData) eventData);

        if (lastElement) {
            process();
        }
    }

    @Override
    public void reset() {
        if (eventCollection == null) {
            return;
        }

        eventCollection.clear();
    }

    private void process() {
        Queue<QueryCacheEventData> eventCollection = this.eventCollection;
        if (eventCollection.isEmpty()) {
            return;
        }
        if (eventCollection.size() < 2) {
            QueryCacheEventData eventData = eventCollection.poll();
            processor.process(eventData);
        } else {
            sendInBatches(eventCollection);
        }
    }

    private void sendInBatches(@Nonnull Queue<QueryCacheEventData> events) {
        Map<Integer, List<QueryCacheEventData>> partitionToEventDataMap = createPartitionToEventDataMap(events);
        sendToSubscriber(partitionToEventDataMap);
    }

    private Map<Integer, List<QueryCacheEventData>> createPartitionToEventDataMap(Queue<QueryCacheEventData> events) {
        if (events.isEmpty()) {
            return Collections.emptyMap();
        }

        final int defaultPartitionCount = Integer.parseInt(ClusterProperty.PARTITION_COUNT.getDefaultValue());
        final int roughSize = Math.min(events.size(), defaultPartitionCount);

        final Map<Integer, List<QueryCacheEventData>> map = createHashMap(roughSize);

        do {
            QueryCacheEventData eventData = events.poll();
            if (eventData == null) {
                break;
            }
            int partitionId = eventData.getPartitionId();

            List<QueryCacheEventData> eventDataList = map.computeIfAbsent(partitionId, k -> new ArrayList<>());
            eventDataList.add(eventData);

        } while (true);

        return map;
    }

    private void sendToSubscriber(Map<Integer, List<QueryCacheEventData>> map) {
        Set<Map.Entry<Integer, List<QueryCacheEventData>>> entries = map.entrySet();
        for (Map.Entry<Integer, List<QueryCacheEventData>> entry : entries) {
            Integer partitionId = entry.getKey();
            List<QueryCacheEventData> eventData = entry.getValue();
            String thisNodesAddress = getThisNodesAddress();
            BatchEventData batchEventData = new BatchEventData(eventData, thisNodesAddress, partitionId);
            processor.process(batchEventData);
        }
    }

    private String getThisNodesAddress() {
        Address thisAddress = context.getThisNodesAddress();
        return thisAddress.toString();
    }
}
