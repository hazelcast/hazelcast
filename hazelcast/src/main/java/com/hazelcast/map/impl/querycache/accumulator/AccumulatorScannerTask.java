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

package com.hazelcast.map.impl.querycache.accumulator;

import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.publisher.MapPublisherRegistry;
import com.hazelcast.map.impl.querycache.publisher.PartitionAccumulatorRegistry;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;
import com.hazelcast.map.impl.querycache.publisher.PublisherRegistry;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * Task for scanning all {@link Accumulator} instances of all {@link com.hazelcast.map.QueryCache}s on this node.
 * <p>
 * If it finds any event that needs to be published in an accumulator, it creates and sends
 * {@link ConsumeAccumulatorOperation} to relevant partitions.
 *
 * @see ConsumeAccumulatorOperation
 */
public class AccumulatorScannerTask implements Runnable {

    private static final int MAX_PROCESSABLE_ACCUMULATOR_COUNT = 10;

    private final ScannerConsumer consumer;
    private final QueryCacheContext context;

    public AccumulatorScannerTask(QueryCacheContext context) {
        this.context = context;
        this.consumer = new ScannerConsumer();
    }

    @Override
    public void run() {
        scanAccumulators();
    }

    void scanAccumulators() {
        PublisherContext publisherContext = context.getPublisherContext();
        MapPublisherRegistry mapPublisherRegistry = publisherContext.getMapPublisherRegistry();
        Map<String, PublisherRegistry> publisherRegistryMap = mapPublisherRegistry.getAll();

        Set<Map.Entry<String, PublisherRegistry>> publishers = publisherRegistryMap.entrySet();
        for (Map.Entry<String, PublisherRegistry> entry : publishers) {
            PublisherRegistry publisherRegistry = entry.getValue();

            Map<String, PartitionAccumulatorRegistry> accumulatorRegistryMap = publisherRegistry.getAll();
            Set<Map.Entry<String, PartitionAccumulatorRegistry>> accumulators = accumulatorRegistryMap.entrySet();
            for (Map.Entry<String, PartitionAccumulatorRegistry> accumulatorRegistryEntry : accumulators) {
                PartitionAccumulatorRegistry accumulatorRegistry = accumulatorRegistryEntry.getValue();
                Map<Integer, Accumulator> accumulatorMap = accumulatorRegistry.getAll();
                for (Map.Entry<Integer, Accumulator> accumulatorEntry : accumulatorMap.entrySet()) {
                    Integer partitionId = accumulatorEntry.getKey();
                    Accumulator accumulator = accumulatorEntry.getValue();
                    int size = accumulator.size();
                    if (size > 0) {
                        consumer.consume(accumulator, partitionId);
                    }
                }
            }
        }

        sendConsumerOperation();
        consumer.reset();
    }

    private void sendConsumerOperation() {
        Map<Integer, Queue<Accumulator>> partitionAccumulators = consumer.getPartitionAccumulators();
        if (partitionAccumulators == null || partitionAccumulators.isEmpty()) {
            return;
        }
        Set<Map.Entry<Integer, Queue<Accumulator>>> entries = partitionAccumulators.entrySet();
        for (Map.Entry<Integer, Queue<Accumulator>> entry : entries) {
            Integer partitionId = entry.getKey();
            Queue<Accumulator> accumulators = entry.getValue();
            if (accumulators.isEmpty()) {
                continue;
            }
            Operation operation = createConsumerOperation(partitionId, accumulators);
            context.getInvokerWrapper().executeOperation(operation);
        }
    }

    private Operation createConsumerOperation(int partitionId, Queue<Accumulator> accumulators) {
        PublisherContext publisherContext = context.getPublisherContext();
        NodeEngineImpl nodeEngine = (NodeEngineImpl) publisherContext.getNodeEngine();

        Operation operation = new ConsumeAccumulatorOperation(accumulators, MAX_PROCESSABLE_ACCUMULATOR_COUNT);
        operation
                .setNodeEngine(nodeEngine)
                .setCallerUuid(nodeEngine.getLocalMember().getUuid())
                .setPartitionId(partitionId)
                .setValidateTarget(false)
                .setService(nodeEngine.getService(MapService.SERVICE_NAME));

        return operation;
    }

    /**
     * Handles collected accumulators in scanning phase.
     */
    private static class ScannerConsumer {

        private Map<Integer, Queue<Accumulator>> partitionAccumulators;

        ScannerConsumer() {
        }

        void consume(Accumulator accumulator, int partitionId) {
            if (partitionAccumulators == null) {
                partitionAccumulators = new HashMap<>();
            }

            Queue<Accumulator> accumulators = partitionAccumulators.get(partitionId);
            if (accumulators == null) {
                accumulators = new ArrayDeque<>();
                partitionAccumulators.put(partitionId, accumulators);
            }
            accumulators.add(accumulator);
        }

        Map<Integer, Queue<Accumulator>> getPartitionAccumulators() {
            return partitionAccumulators;
        }


        void reset() {
            if (partitionAccumulators != null) {
                partitionAccumulators.clear();
            }
        }
    }
}
