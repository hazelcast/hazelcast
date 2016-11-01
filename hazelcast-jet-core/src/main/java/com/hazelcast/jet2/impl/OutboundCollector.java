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
import java.util.List;

interface OutboundCollector {
    /**
     * Offers an item to this collector.
     * If the collector cannot complete the operation, the call must be retried later.
     */
    ProgressState offer(Object item);

    /**
     * Tries to close this edge collector.
     * If the stream cannot complete the operation now, the call must be retried later.
     */
    ProgressState close();

    /**
     * Returns the list of partitions handled by this collector
     */
    List<Integer> getPartitions();


    static OutboundCollector compositeCollector(OutboundCollector[] collectors, EdgeDef outboundEdge) {
        switch (outboundEdge.getForwardingPattern()) {
            case ALL_TO_ONE:
                throw new RuntimeException("to implement");
            case ALTERNATING_SINGLE:
                return new RoundRobin(collectors);
            case PARTITIONED:
                return new Partitioned(collectors, outboundEdge.getPartitioner());
            case BROADCAST:
                return new Broadcast(collectors);
            default:
                throw new AssertionError("Missing case label for " + outboundEdge.getForwardingPattern());
        }
    }

    abstract class Composite implements OutboundCollector {

        protected final OutboundCollector[] collectors;
        protected final ProgressTracker progTracker = new ProgressTracker();

        Composite(OutboundCollector[] collectors) {
            this.collectors = collectors;
        }

        @Override
        public ProgressState close() {
            progTracker.reset();
            for (int i = 0; i < collectors.length; i++) {
                if (collectors[i] == null) {
                    continue;
                }
                ProgressState result = collectors[i].close();
                progTracker.update(result);
                if (result.isDone()) {
                    collectors[i] = null;
                }
            }
            return progTracker.toProgressState();
        }
    }
}

class RoundRobin extends OutboundCollector.Composite {

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

    @Override
    public List<Integer> getPartitions() {
        return null;
    }
}

class Broadcast extends OutboundCollector.Composite {
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
            progTracker.update(result);
            if (result.isDone()) {
                isItemSentTo.set(i);
            }
        }
        if (progTracker.isDone()) {
            isItemSentTo.clear();
        }
        return progTracker.toProgressState();
    }

    @Override
    public List<Integer> getPartitions() {
        return null;
    }
}

class Partitioned extends OutboundCollector.Composite {

    private final Partitioner partitioner;

    Partitioned(OutboundCollector[] collectors, Partitioner partitioner) {
        super(collectors);
        this.partitioner = partitioner;
    }

    @Override
    public ProgressState offer(Object item) {
        int partition = partitioner.getPartition(item, collectors.length);
        assert partition >= 0 && partition < collectors.length : "Partition number out of range";
        return collectors[partition].offer(item);
    }

    @Override
    public List<Integer> getPartitions() {
        return null;
    }

}
