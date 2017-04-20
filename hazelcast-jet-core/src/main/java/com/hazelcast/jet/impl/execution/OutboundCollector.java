/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.Partitioner;
import com.hazelcast.jet.impl.execution.init.EdgeDef;
import com.hazelcast.jet.impl.util.CircularListCursor;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;
import java.util.BitSet;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public interface OutboundCollector {
    /**
     * Offers an item to this collector.
     * If the collector cannot complete the operation, the call must be retried later.
     */
    ProgressState offer(Object item);

    /**
     * Offer a punctuation to this collector. Punctuations will be propagated to all sub-collectors
     * if the collector is a composite one.
     */
    ProgressState offerBroadcast(Object item);

    /**
     * Offers an item with a known partition id
     */
    default ProgressState offer(Object item, int partitionId) {
        return offer(item);
    }

    /**
     * Returns the list of partitions handled by this collector.
     */
    int[] getPartitions();


    static OutboundCollector compositeCollector(
            OutboundCollector[] collectors, EdgeDef outboundEdge, int partitionCount
    ) {
        if (collectors.length == 1) {
            return collectors[0];
        }
        switch (outboundEdge.forwardingPattern()) {
            case VARIABLE_UNICAST:
            case ONE_TO_MANY:
                return new RoundRobin(collectors);
            case PARTITIONED:
                return new Partitioned(collectors, outboundEdge.partitioner(), partitionCount);
            case BROADCAST:
                return new Broadcast(collectors);
            default:
                throw new AssertionError("Missing case label for " + outboundEdge.forwardingPattern());
        }
    }

    abstract class Composite implements OutboundCollector {

        protected final OutboundCollector[] collectors;
        protected final int[] partitions;
        protected final ProgressTracker progTracker = new ProgressTracker();
        protected final BitSet broadcastTracker;

        Composite(OutboundCollector[] collectors) {
            this.collectors = collectors;
            this.broadcastTracker = new BitSet(collectors.length);
            this.partitions = Stream.of(collectors)
                                    .flatMapToInt(c -> IntStream.of(c.getPartitions()))
                                    .sorted().toArray();
        }

        @Override
        public ProgressState offerBroadcast(Object punc) {
            progTracker.reset();
            for (int i = 0; i < collectors.length; i++) {
                if (broadcastTracker.get(i)) {
                    continue;
                }
                ProgressState result = collectors[i].offerBroadcast(punc);
                progTracker.mergeWith(result);
                if (result.isDone()) {
                    broadcastTracker.set(i);
                }
            }
            if (progTracker.isDone()) {
                broadcastTracker.clear();
            }
            return progTracker.toProgressState();
        }

        @Override
        @SuppressFBWarnings("EI_EXPOSE_REP")
        public int[] getPartitions() {
            return partitions;
        }
    }

    class RoundRobin extends Composite {

        private final CircularListCursor<OutboundCollector> cursor;

        RoundRobin(OutboundCollector[] collectors) {
            super(collectors);
            this.cursor = new CircularListCursor<>(Arrays.asList(collectors));
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

        Broadcast(OutboundCollector[] collectors) {
            super(collectors);
        }

        @Override
        public ProgressState offer(Object item) {
            progTracker.reset();
            for (int i = 0; i < collectors.length; i++) {
                if (broadcastTracker.get(i)) {
                    continue;
                }
                ProgressState result = collectors[i].offer(item);
                progTracker.mergeWith(result);
                if (result.isDone()) {
                    broadcastTracker.set(i);
                }
            }
            if (progTracker.isDone()) {
                broadcastTracker.clear();
            }
            return progTracker.toProgressState();
        }
    }

    class Partitioned extends Composite {

        private final Partitioner partitioner;
        private final OutboundCollector[] partitionLookupTable;
        private int partitionId = -1;

        Partitioned(OutboundCollector[] collectors, Partitioner partitioner, int partitionCount) {
            super(collectors);
            this.partitioner = partitioner;
            this.partitionLookupTable = new OutboundCollector[partitionCount];

            for (OutboundCollector collector : collectors) {
                for (int partitionId : collector.getPartitions()) {
                    partitionLookupTable[partitionId] = collector;
                }
            }
        }

        @Override
        public ProgressState offer(Object item) {
            if (partitionId == -1) {
                partitionId = partitioner.getPartition(item, partitionLookupTable.length);
                assert partitionId >= 0 && partitionId < partitionLookupTable.length
                        : "Partition number out of range: " + partitionId + ", offending item: " + item;
            }
            ProgressState result = offer(item, partitionId);
            if (result.isDone()) {
                partitionId = -1;
            }
            return result;
        }

        @Override
        public ProgressState offer(Object item, int partitionId) {
            OutboundCollector collector = partitionLookupTable[partitionId];
            assert collector != null : "This item should not be handled by this collector as "
                    + "requested partitionId is not present";
            return collector.offer(item, partitionId);
        }
    }
}


