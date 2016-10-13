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

import com.hazelcast.internal.util.concurrent.ConcurrentConveyor;
import com.hazelcast.jet2.Edge;
import com.hazelcast.jet2.Partitioner;
import com.hazelcast.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;

import static com.hazelcast.jet2.impl.DoneItem.DONE_ITEM;
import static com.hazelcast.jet2.impl.ProgressState.DONE;
import static com.hazelcast.jet2.impl.ProgressState.NO_PROGRESS;

/**
 * Javadoc pending.
 */
abstract class ConcurrentOutboundEdgeStream implements OutboundEdgeStream {
    protected final int queueIndex;
    protected final int ordinal;

    protected final ProgressTracker progTracker = new ProgressTracker();

    protected ConcurrentOutboundEdgeStream(int queueIndex, int ordinal) {
        Preconditions.checkTrue(queueIndex >= 0, "queue index must be positive");

        this.queueIndex = queueIndex;
        this.ordinal = ordinal;
    }

    @Override
    public int ordinal() {
        return ordinal;
    }

    private static void validateConveyors(ConcurrentConveyor<Object>[] conveyors, int queueIndex) {
        Preconditions.checkTrue(conveyors.length > 0, "Conveyor array is empty");
        Preconditions.checkTrue(queueIndex >= 0 && queueIndex < conveyors[0].queueCount(),
                "The given queue index is out of range for the given conveyor array");
    }

    private static class RoundRobin extends ConcurrentOutboundEdgeStream {

        private final CircularCursor<ConcurrentConveyor<Object>> cursor;

        public RoundRobin(ConcurrentConveyor<Object>[] conveyors, int queueIndex, int ordinal) {
            super(queueIndex, ordinal);
            validateConveyors(conveyors, queueIndex);
            this.cursor = new CircularCursor<>(new ArrayList<>(Arrays.asList(conveyors)));
        }

        @Override
        public ProgressState offer(Object item) {
            final ConcurrentConveyor<Object> first = cursor.value();
            do {
                boolean accepted = cursor.value().offer(queueIndex, item);
                cursor.advance();
                if (accepted) {
                    return DONE;
                }
            } while (cursor.value() != first);
            return NO_PROGRESS;
        }

        @Override
        public ProgressState close() {
            progTracker.reset();
            final ConcurrentConveyor<Object> first = cursor.value();
            do {
                final ConcurrentConveyor<Object> c = cursor.value();
                if (c.offer(queueIndex, DONE_ITEM)) {
                    progTracker.madeProgress();
                    cursor.remove();
                } else {
                    progTracker.notDone();
                }
            } while (cursor.advance() && cursor.value() != first);
            return progTracker.toProgressState();
        }
    }

    private static class Broadcast extends ConcurrentOutboundEdgeStream {

        protected final ConcurrentConveyor<Object>[] conveyors;
        private final BitSet isItemSentTo;

        public Broadcast(ConcurrentConveyor<Object>[] conveyors, int queueIndex, int ordinal) {
            super(queueIndex, ordinal);
            validateConveyors(conveyors, queueIndex);
            this.conveyors = conveyors.clone();
            this.isItemSentTo = new BitSet(conveyors.length);
        }

        @Override
        public ProgressState offer(Object item) {
            return broadcast(item);
        }

        @Override
        public ProgressState close() {
            return broadcast(DONE_ITEM);
        }

        protected final ProgressState broadcast(Object item) {
            progTracker.reset();
            for (int i = 0; i < conveyors.length; i++) {
                if (isItemSentTo.get(i)) {
                    continue;
                }
                if (conveyors[i].offer(queueIndex, item)) {
                    progTracker.madeProgress();
                    isItemSentTo.set(i);
                } else {
                    progTracker.notDone();
                }
            }
            if (progTracker.isDone()) {
                isItemSentTo.clear();
            }
            return progTracker.toProgressState();
        }
    }

    private static class Partitioned extends Broadcast {

        private final Partitioner partitioner;

        public Partitioned(ConcurrentConveyor<Object>[] conveyors, Partitioner partitioner,
                           int queueIndex, int ordinal) {
            super(conveyors, queueIndex, ordinal);
            validateConveyors(conveyors, queueIndex);

            this.partitioner = partitioner;
        }

        @Override
        public ProgressState offer(Object item) {
            int partition = partitioner.getPartition(item, conveyors.length);
            assert partition >= 0 && partition < conveyors.length;
            return conveyors[partition].offer(queueIndex, item) ? DONE : NO_PROGRESS;
        }
    }

    public static OutboundEdgeStream newStream(
            ConcurrentConveyor<Object>[] conveyors, Edge edge, int taskletIndex) {
        int ordinal = edge.getOutputOrdinal();
        switch (edge.getForwardingPattern()) {
            case SINGLE:
                return new RoundRobin(conveyors, taskletIndex, ordinal);
            case PARTITIONED:
                return new Partitioned(conveyors, edge.getPartitioner(), taskletIndex, ordinal);
            case BROADCAST:
                return new Broadcast(conveyors, taskletIndex, ordinal);
            default:
                throw new AssertionError("Missing case label for " + edge.getForwardingPattern());
        }
    }
}
