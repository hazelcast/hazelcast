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
import com.hazelcast.jet.impl.util.CircularCursor;
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
        switch (outboundEdge.forwardingPattern()) {
            case VARIABLE_UNICAST:
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
        @SuppressFBWarnings("EI_EXPOSE_REP")
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
                assert partitionId >= 0 && partitionId < partitionLookupTable.length : "Partition number out of range";
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


