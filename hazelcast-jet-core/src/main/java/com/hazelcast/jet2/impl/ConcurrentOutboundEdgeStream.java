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

/**
 * Javadoc pending.
 */
abstract class ConcurrentOutboundEdgeStream implements OutboundEdgeStream {
    protected final int queueIndex;
    protected final int ordinal;

    protected final ProgressTracker tracker = new ProgressTracker();

    public ConcurrentOutboundEdgeStream(int queueIndex, int ordinal) {
        Preconditions.checkTrue(queueIndex >= 0, "queue index must be positive");

        this.queueIndex = queueIndex;
        this.ordinal = ordinal;
    }

    @Override
    public ProgressState offer(Object item) {
        if (item == DONE_ITEM) {
            return complete();
        }
        return tryOffer(item);
    }

    @Override
    public int ordinal() {
        return ordinal;
    }

    protected abstract ProgressState complete();

    protected abstract ProgressState tryOffer(Object item);

    private static class RoundRobin extends ConcurrentOutboundEdgeStream {

        private final CircularCursor<ConcurrentConveyor<Object>> cursor;

        public RoundRobin(ConcurrentConveyor<Object>[] conveyors, int queueIndex, int ordinal) {
            super(queueIndex, ordinal);
            validateConveyors(conveyors, queueIndex);

            this.cursor = new CircularCursor<>(new ArrayList<>(Arrays.asList(conveyors)));
        }

        @Override
        protected ProgressState tryOffer(Object item) {
            ConcurrentConveyor<Object> first = cursor.value();
            do {
                boolean offered = cursor.value().offer(queueIndex, item);
                cursor.advance();
                if (offered) {
                    return DONE;
                }
            } while (cursor.value() != first);
            return ProgressState.NO_PROGRESS;
        }

        @Override
        protected ProgressState complete() {
            tracker.reset();
            ConcurrentConveyor<Object> first = cursor.value();
            do {
                ConcurrentConveyor<Object> conveyor = cursor.value();
                if (conveyor.offer(queueIndex, DONE_ITEM)) {
                    tracker.update(DONE);
                    cursor.remove();
                } else {
                    tracker.notDone();
                }
            } while (cursor.advance() && cursor.value() != first);
            return tracker.toProgressState();
        }
    }

    private static void validateConveyors(ConcurrentConveyor<Object>[] conveyors, int queueIndex) {
        Preconditions.checkTrue(conveyors.length > 0, "There must be at least one conveyor in the array");
        Preconditions.checkTrue(queueIndex < conveyors[0].queueCount(),
                "Queue index must be less than number of queues in each conveyor");
    }

    private static class Broadcast extends ConcurrentOutboundEdgeStream {

        protected final ConcurrentConveyor<Object>[] conveyors;
        private final BitSet isItemBroadcast;

        public Broadcast(ConcurrentConveyor<Object>[] conveyors, int queueIndex, int ordinal) {
            super(queueIndex, ordinal);
            validateConveyors(conveyors, queueIndex);


            this.conveyors = conveyors.clone();
            this.isItemBroadcast = new BitSet(conveyors.length);
        }

        @Override
        protected ProgressState complete() {
            tracker.reset();
            for (int i = 0; i < conveyors.length; i++) {
                if (!isItemBroadcast.get(i)) {
                    if (conveyors[i].offer(queueIndex, DONE_ITEM)) {
                        tracker.update(DONE);
                        conveyors[i] = null;
                        isItemBroadcast.set(i);
                    } else {
                        tracker.notDone();
                    }
                }
            }
            if (tracker.isDone()) {
                isItemBroadcast.clear();
            }
            return tracker.toProgressState();
        }

        @Override
        protected ProgressState tryOffer(Object item) {
            tracker.reset();
            for (int i = 0; i < conveyors.length; i++) {
                if (!isItemBroadcast.get(i)) {
                    if (conveyors[i].offer(queueIndex, item)) {
                        tracker.update(DONE);
                        isItemBroadcast.set(i);
                    } else {
                        tracker.notDone();
                    }
                }
            }
            if (tracker.isDone()) {
                isItemBroadcast.clear();
            }
            return tracker.toProgressState();
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
        protected ProgressState tryOffer(Object item) {
            int partition = partitioner.getPartition(item, conveyors.length);
            assert partition >= 0 && partition < conveyors.length;
            if (conveyors[partition].offer(queueIndex, item)) {
                return ProgressState.DONE;
            } else {
                return ProgressState.NO_PROGRESS;
            }
        }
    }

    public static OutboundEdgeStream newStream(ConcurrentConveyor<Object>[] conveyors, Edge edge,
                                               int taskletIndex) {
        int ordinal = edge.getOutputOrdinal();
        switch (edge.getForwardingPattern()) {
            case SINGLE:
                return new RoundRobin(conveyors, taskletIndex, ordinal);
            case PARTITIONED:
                return new Partitioned(conveyors, edge.getPartitioner(), taskletIndex, ordinal);
            case BROADCAST:
                return new Broadcast(conveyors, taskletIndex, ordinal);
            default:
                throw new IllegalArgumentException("impossible");
        }
    }
}
