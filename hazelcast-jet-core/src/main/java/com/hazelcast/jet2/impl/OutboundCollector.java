/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet2.impl;

import com.hazelcast.jet2.Partitioner;
import java.util.Arrays;
import java.util.BitSet;
import java.util.stream.IntStream;
import java.util.stream.Stream;

interface OutboundCollector {
    /**
     * Offers an item to this collector.
     * If the collector cannot complete the operation, the call must be retried later.
     */
    ProgressState offer(Object item);

    /**
     * Offers an item with a known partition id
     */
    default ProgressState offer(Object item, int partitionId) {
        return offer(item);
    }

    /**
     * Tries to close this collector.
     * If the collector didn't complete the operation now, the call must be retried later.
     */
    ProgressState close();

    /**
     * Returns the list of partitions handled by this collector.
     */
    int[] getPartitions();


    static OutboundCollector compositeCollector(
            OutboundCollector[] collectors, EdgeDef outboundEdge, int partitionCount
    ) {
        switch (outboundEdge.getForwardingPattern()) {
            case ALTERNATING_SINGLE:
                return new RoundRobin(collectors);
            case PARTITIONED:
                return new Partitioned(collectors, outboundEdge.getPartitioner(), partitionCount);
            case BROADCAST:
                return new Broadcast(collectors);
            default:
                throw new AssertionError("Missing case label for " + outboundEdge.getForwardingPattern());
        }
    }

    abstract class Composite implements OutboundCollector {

        protected final OutboundCollector[] collectors;
        protected final ProgressTracker progTracker = new ProgressTracker();
        protected final int[] partitions;

        Composite(OutboundCollector[] collectors) {
            this.collectors = collectors;
            this.partitions = Stream.of(collectors)
                                    .flatMapToInt(c -> IntStream.of(c.getPartitions()))
                                    .sorted().toArray();
        }

        @Override
        public ProgressState close() {
            progTracker.reset();
            for (int i = 0; i < collectors.length; i++) {
                if (collectors[i] == null) {
                    continue;
                }
                ProgressState result = collectors[i].close();
                progTracker.mergeWith(result);
                if (result.isDone()) {
                    collectors[i] = null;
                }
            }
            return progTracker.toProgressState();
        }

        @Override
        public int[] getPartitions() {
            return partitions;
        }
    }

    class RoundRobin extends Composite {

        private final CircularCursor<OutboundCollector> cursor;

        RoundRobin(OutboundCollector[] collectors) {
            super(collectors);
            this.cursor = new CircularCursor<>(Arrays.asList(collectors));
        }

        @Override
        public ProgressState offer(Object item) {
            final OutboundCollector first = cursor.value();
            ProgressState result;
            do {
                result = cursor.value().offer(item);
                if (result.isDone()) {
                    cursor.advance();
                    return result;
                } else if (result.isMadeProgress()) {
                    return result;
                }
                cursor.advance();
            } while (cursor.value() != first);
            return result;
        }
    }

    class Broadcast extends Composite {
        private final ProgressTracker progTracker = new ProgressTracker();
        private final BitSet isItemSentTo;

        Broadcast(OutboundCollector[] collectors) {
            super(collectors);
            this.isItemSentTo = new BitSet(collectors.length);
        }

        @Override
        public ProgressState offer(Object item) {
            progTracker.reset();
            for (int i = 0; i < collectors.length; i++) {
                if (isItemSentTo.get(i)) {
                    continue;
                }
                ProgressState result = collectors[i].offer(item);
                progTracker.mergeWith(result);
                if (result.isDone()) {
                    isItemSentTo.set(i);
                }
            }
            if (progTracker.isDone()) {
                isItemSentTo.clear();
            }
            return progTracker.toProgressState();
        }
    }

    class Partitioned extends Composite {

        private final Partitioner partitioner;
        private final OutboundCollector[] partitionLookupTable;

        Partitioned(OutboundCollector[] collectors, Partitioner partitioner, int partitionCount) {
            super(collectors);
            this.partitioner = partitioner;
            this.partitionLookupTable = new OutboundCollector[partitionCount];

            for (OutboundCollector collector : collectors) {
                for (Integer integer : collector.getPartitions()) {
                    partitionLookupTable[integer] = collector;
                }
            }
        }

        @Override
        public ProgressState offer(Object item) {
            int partition = partitioner.getPartition(item, partitionLookupTable.length);
            assert partition >= 0 && partition < partitionLookupTable.length : "Partition number out of range";
            return offer(item, partition);
        }

        @Override
        public ProgressState offer(Object item, int partitionId) {
            OutboundCollector collector = partitionLookupTable[partitionId];
            assert collector != null : "This item should not be handled by this collector as " +
                    "requested partition is not present";
            return collector.offer(item, partitionId);
        }
    }

    class Diagnostic implements OutboundCollector {

        private final String name;
        private final OutboundCollector collector;
        private int counter = 0;

        public Diagnostic(String name, OutboundCollector collector) {
            this.name = name;
            this.collector = collector;
        }

        @Override
        public ProgressState offer(Object item) {
            ProgressState offered = collector.offer(item);
            if (offered == ProgressState.DONE) {
                counter++;
            }
            return offered;
        }

        @Override
        public ProgressState close() {
            System.out.println(name + ": " + counter);
            return collector.close();
        }

        @Override
        public int[] getPartitions() {
            return collector.getPartitions();
        }
    }
}


