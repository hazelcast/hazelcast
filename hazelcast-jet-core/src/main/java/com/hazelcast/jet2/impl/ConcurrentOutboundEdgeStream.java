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

import com.hazelcast.jet2.Edge;
import com.hazelcast.jet2.Partitioner;
import com.hazelcast.util.Preconditions;

import java.util.Arrays;
import java.util.BitSet;

import static com.hazelcast.jet2.impl.DoneItem.DONE_ITEM;
import static com.hazelcast.jet2.impl.ProgressState.DONE;
import static com.hazelcast.jet2.impl.ProgressState.NO_PROGRESS;

/**
 * {@code OutboundEdgeStream} implemented in terms of an array of {@code ConcurrentConveyor}s,
 * one per downstream tasklet. An instance of this class is associated with a unique queue index
 * and writes only to that index of all the {@code ConcurrentConveyor}s.
 */
abstract class ConcurrentOutboundEdgeStream implements OutboundEdgeStream {

    protected final OutboundConsumer[] consumers;
    protected final int ordinal;

    private final ProgressTracker progTracker = new ProgressTracker();
    private final BitSet isItemSentTo;

    protected ConcurrentOutboundEdgeStream(OutboundConsumer[] consumers, int ordinal) {
        Preconditions.checkTrue(consumers.length > 0, "Consumer array is empty");

        this.consumers = consumers;
        this.isItemSentTo = new BitSet(this.consumers.length);
        this.ordinal = ordinal;
    }

    public static OutboundEdgeStream newStream(
            OutboundConsumer[] consumers, Edge edge) {
        int ordinal = edge.getOutputOrdinal();
        switch (edge.getForwardingPattern()) {
            case ALTERNATING_SINGLE:
                return new RoundRobin(consumers, ordinal);
            case PARTITIONED:
                return new Partitioned(consumers, edge.getPartitioner(), ordinal);
            case BROADCAST:
                return new Broadcast(consumers, ordinal);
            default:
                throw new AssertionError("Missing case label for " + edge.getForwardingPattern());
        }
    }

    @Override
    public int ordinal() {
        return ordinal;
    }

    @Override
    public ProgressState close() {
        return broadcast(DONE_ITEM);
    }

    protected final ProgressState broadcast(Object item) {
        progTracker.reset();
        for (int i = 0; i < consumers.length; i++) {
            if (isItemSentTo.get(i)) {
                continue;
            }
            if (consumers[i].offer(item)) {
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

    private static class RoundRobin extends ConcurrentOutboundEdgeStream {

        private final CircularCursor<OutboundConsumer> cursor;

        RoundRobin(OutboundConsumer[] consumers, int ordinal) {
            super(consumers, ordinal);
            this.cursor = new CircularCursor<>(Arrays.asList(this.consumers));
        }

        @Override
        public ProgressState offer(Object item) {
            final OutboundConsumer first = cursor.value();
            do {
                boolean accepted = cursor.value().offer(item);
                cursor.advance();
                if (accepted) {
                    return DONE;
                }
            } while (cursor.value() != first);
            return NO_PROGRESS;
        }
    }

    private static class Broadcast extends ConcurrentOutboundEdgeStream {
        Broadcast(OutboundConsumer[] consumers, int ordinal) {
            super(consumers, ordinal);
        }

        @Override
        public ProgressState offer(Object item) {
            return broadcast(item);
        }
    }

    private static class Partitioned extends Broadcast {

        private final Partitioner partitioner;

        public Partitioned(OutboundConsumer[] consumers, Partitioner partitioner, int ordinal) {
            super(consumers, ordinal);
            this.partitioner = partitioner;
        }

        @Override
        public ProgressState offer(Object item) {
            int partition = partitioner.getPartition(item, consumers.length);
            assert partition >= 0 && partition < consumers.length : "Partition number out of range";
            return consumers[partition].offer(item) ? DONE : NO_PROGRESS;
        }
    }
}
