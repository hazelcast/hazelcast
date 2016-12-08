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

package com.hazelcast.jet.impl;

import com.hazelcast.internal.util.concurrent.MPSCQueue;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.util.concurrent.IdleStrategy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLongArray;

import static com.hazelcast.jet.impl.DoneItem.DONE_ITEM;
import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * Receives from all remote members the data associated with a single edge.
 */
class ReceiverTasklet implements Tasklet {

    /**
     * The {@code itemSeqsPerSender} array holds, per sending member, the sequence number
     * acknowledged to the sender as having been received and processed. The sequence
     * increments in terms of the estimated heap occupancy of each received item, in bytes.
     * However, to save on network traffic, the number reported to the sender is
     * coarser-grained: it counts in units of {@code 1 << COMPRESSED_SEQ_SHIFT}. For example,
     * with the value 20 the unit is one megabyte.
     */
    static final int COMPRESSED_SEQ_UNIT_LOG2 = 20;

    /**
     * The Receive Window, in analogy to TCP's RWIN, is the number of seq units the
     * sender can be ahead of the acknowledged seq. The correspondence between a seq unit and
     * bytes is defined by the constant {@link #COMPRESSED_SEQ_UNIT_LOG2}.
     */
    static final int RECEIVE_WINDOW_COMPRESSED = 100;

    private final Queue<PacketWithSender> incoming = new MPSCQueue<>((IdleStrategy) null);
    private final ProgressTracker tracker = new ProgressTracker();
    private final ArrayDeque<ObjPtionAndSenderId> inbox = new ArrayDeque<>();
    private final OutboundCollector collector;
    private final AtomicLongArray itemSeqsPerSender;

    private int remainingSenders;

    ReceiverTasklet(OutboundCollector collector, int senderCount) {
        this.collector = collector;
        this.remainingSenders = senderCount;
        this.itemSeqsPerSender = new AtomicLongArray(senderCount);
    }

    void addPacket(BufferObjectDataInput packetInput, int senderId) {
        incoming.add(new PacketWithSender(senderId, packetInput));
    }

    int ackedSeq(int senderId) {
        return compressSeq(itemSeqsPerSender.get(senderId));
    }

    @Override
    public ProgressState call() {
        tracker.reset();
        tryFillInbox();
        for (ObjPtionAndSenderId o; (o = inbox.peek()) != null; ) {
            final Object item = o.getItem();
            if (item == DONE_ITEM) {
                remainingSenders--;
            } else {
                ProgressState outcome = collector.offer(item, o.getPartitionId());
                if (!outcome.isDone()) {
                    tracker.madeProgress(outcome.isMadeProgress());
                    break;
                }
            }
            tracker.madeProgress();
            inbox.remove();
            final long seq = itemSeqsPerSender.get(o.senderId);
            itemSeqsPerSender.lazySet(o.senderId, seq + o.estimatedMemoryFootprint);
        }
        if (remainingSenders == 0) {
            tracker.mergeWith(collector.close());
        } else {
            tracker.notDone();
        }
        return tracker.toProgressState();
    }

    static int compressSeq(long seq) {
        return (int) (seq >> COMPRESSED_SEQ_UNIT_LOG2);
    }

    static long estimatedMemoryFootprint(int itemBlobSize) {
        final int inboxSlot = 4; // slot in ArrayDeque<ObjPtionAndSenderId> inbox
        final int objPtionAndSenderIdHeader = 16; // object header of ObjPtionAndSenderId instance
        final int itemField = 4; // ObjectWithPartitionId.item
        final int itemObjHeader = 16; // header of the item object (unknown type)
        final int partitionIdField = 4; // ObjectWithPartitionId.item
        final int senderIdField = 4; // ObjectWithPartitionId.senderId
        final int estimatedMemoryFootprintField = 8;  // ObjectWithPartitionId.estimatedMemoryFootprint
        final int overhead = inboxSlot + objPtionAndSenderIdHeader + itemField + itemObjHeader + partitionIdField
                + senderIdField + estimatedMemoryFootprintField;
        return overhead + itemBlobSize;
    }

    private void tryFillInbox() {
        try {
            for (PacketWithSender received; (received = incoming.poll()) != null; ) {
                final BufferObjectDataInput in = received.payload;
                final int itemCount = in.readInt();
                for (int i = 0; i < itemCount; i++) {
                    final int mark = in.position();
                    final Object item = in.readObject();
                    final int itemSize = in.position() - mark;
                    inbox.add(new ObjPtionAndSenderId(item, in.readInt(), received.senderId, itemSize));
                }
                tracker.madeProgress();
            }
        } catch (IOException e) {
            throw rethrow(e);
        }
    }

    private static class PacketWithSender {
        final int senderId;
        final BufferObjectDataInput payload;

        PacketWithSender(int senderId, BufferObjectDataInput payload) {
            this.senderId = senderId;
            this.payload = payload;
        }
    }

    private static class ObjPtionAndSenderId extends ObjectWithPartitionId {
        final int senderId;
        final long estimatedMemoryFootprint;

        ObjPtionAndSenderId(Object item, int partitionId, int senderId, int itemBlobSize) {
            super(item, partitionId);
            this.senderId = senderId;
            this.estimatedMemoryFootprint = estimatedMemoryFootprint(itemBlobSize);
        }

    }
}
