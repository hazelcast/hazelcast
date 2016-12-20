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
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLongArray;

import static com.hazelcast.jet.impl.DoneItem.DONE_ITEM;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.lang.Math.ceil;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Receives from all remote members the data associated with a single edge.
 */
class ReceiverTasklet implements Tasklet {

    /**
     * The {@code ackedSeq} atomic array holds, per sending member, the sequence number
     * acknowledged to the sender as having been received and processed. The sequence
     * increments in terms of the estimated heap occupancy of each received item, in bytes.
     * However, to save on network traffic, the number reported to the sender is
     * coarser-grained: it counts in units of {@code 1 << COMPRESSED_SEQ_UNIT_LOG2}. For example,
     * with a value of 20 the unit would be one megabyte. The coarse-grained seq is called "compressed seq".
     */
    static final int COMPRESSED_SEQ_UNIT_LOG2 = 16;
    /**
     * The Receive Window, in analogy to TCP's RWIN, is the number of compressed seq units the
     * sender can be ahead of the acknowledged seq. The correspondence between a compresesd seq unit
     * and bytes is defined by the constant {@link ReceiverTasklet#COMPRESSED_SEQ_UNIT_LOG2}.
     * <p>
     * The receiver tasklet keeps an array of receive window sizes, one for each sender.
     * <p>
     * This constant specifies the initial size of the receive window. The window is constantly
     * adapted according to the actual data flow through the receiver tasklet.
     */
    static final int INITIAL_RECEIVE_WINDOW_COMPRESSED = 800;
    /**
     * Receive Window converges towards the amount of data processed per flow-control period multiplied by this number.
     */
    static final int RWIN_MULTIPLIER = 3;

    static final double FLOW_CONTROL_PERIOD_NS = (double) MILLISECONDS.toNanos(JetService.FLOW_CONTROL_PERIOD_MS);


    private final Queue<PacketWithSender> incoming = new MPSCQueue<>((IdleStrategy) null);
    private final ProgressTracker tracker = new ProgressTracker();
    private final ArrayDeque<ObjPtionAndSenderId> inbox = new ArrayDeque<>();
    private final OutboundCollector collector;

    private int remainingSenders;


    //                    FLOW-CONTROL STATE
    //            All arrays are indexed by sender ID.

    // read by a task scheduler thread, written by a tasklet execution thread
    private final AtomicLongArray ackedSeq;

    // read and written by updateAndGetSendSeqLimitCompressed(), which is invoked sequentially by a task scheduler
    private final int[] receiveWindowCompressed;
    private final int[] prevAckedSeqCompressed;
    private final long[] prevTimestamp;

    //                 END FLOW-CONTROL STATE

    ReceiverTasklet(OutboundCollector collector, int senderCount) {
        this.collector = collector;
        this.remainingSenders = senderCount;
        this.ackedSeq = new AtomicLongArray(senderCount);
        this.receiveWindowCompressed = new int[senderCount];
        Arrays.fill(receiveWindowCompressed, INITIAL_RECEIVE_WINDOW_COMPRESSED);
        this.prevAckedSeqCompressed = new int[senderCount];
        this.prevTimestamp = new long[senderCount];
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
            ackItem(o.senderId, o.estimatedMemoryFootprint);
        }
        if (remainingSenders == 0) {
            tracker.mergeWith(collector.close());
        } else {
            tracker.notDone();
        }
        return tracker.toProgressState();
    }

    void receiveStreamPacket(BufferObjectDataInput packetInput, int senderId) {
        incoming.add(new PacketWithSender(senderId, packetInput));
    }

    /**
     * Calls {@link #updateAndGetSendSeqLimitCompressed(int, long)} with {@code System.nanotime()}
     * and the current acked seq for the given sender ID.
     */
    int updateAndGetSendSeqLimitCompressed(int senderId) {
        return updateAndGetSendSeqLimitCompressed(senderId, System.nanoTime());
    }

    /**
     * Calculates the upper limit for the compressed form of {@link SenderTasklet#sentSeq}, which
     * constrains how much more data the remote sender tasklet can send to this tasklet.
     * Steps to calculate the limit:
     * <ol><li>
     *     Calculate the following:
     *     <ol type="a"><li>
     *         {@code timeDelta} = difference between the timestamps of this and previous method call
     *     </li><li>
     *         {@code seqDelta} = amount of data processed by the receiver between the calls, measured in
     *         compressed seq units (see {@link #COMPRESSED_SEQ_UNIT_LOG2})
     *     </li><li>
     *         {@code seqsPerAckPeriod = (seqDelta / timeDelta) * }{@link JetService#FLOW_CONTROL_PERIOD_MS},
     *         projected amount of data processed by the receiver in one standard flow control period
     *     </li></ol>
     * </li><li>
     *     Define the <emph>target receive window</emph> as {@code 3 * seqsPerAckPeriod}.
     * </li><li>
     *     Adjust the current receive window halfway toward the target receive window.
     * </li><li>
     *     Return the {@code sentSeq} limit as the current acked seq plus the current receive window.
     * </li></ol>
     *
     * @param senderId ID of the member whose {@code sentSeq} limit to update and return
     * @param timestampNow value of the timestamp at the time the method is called. The timestamp must be
 *                     obtained from {@code System.nanoTime()}.
     */
    // Invoked sequentially by a task scheduler
    int updateAndGetSendSeqLimitCompressed(int senderId, long timestampNow) {
        final boolean hadPrevStats = prevTimestamp[senderId] != 0 || prevAckedSeqCompressed[senderId] != 0;

        final long ackTimeDelta = timestampNow - prevTimestamp[senderId];
        prevTimestamp[senderId] = timestampNow;

        final int ackedSeqCompressed = compressSeq(ackedSeq.get(senderId));
        final int ackedSeqCompressedDelta = ackedSeqCompressed - prevAckedSeqCompressed[senderId];
        prevAckedSeqCompressed[senderId] = ackedSeqCompressed;

        if (hadPrevStats) {
            final double ackedSeqsPerAckPeriod = FLOW_CONTROL_PERIOD_NS * ackedSeqCompressedDelta / ackTimeDelta;
            final int targetRwin = RWIN_MULTIPLIER * (int) ceil(ackedSeqsPerAckPeriod);
            final int rwinDiff = targetRwin - receiveWindowCompressed[senderId];
            receiveWindowCompressed[senderId] += rwinDiff / 2;
        }
        return ackedSeqCompressed + receiveWindowCompressed[senderId];
    }

    long ackItem(int senderId, long itemWeight) {
        final long seqNow = ackedSeq.get(senderId);
        final long seqToBe = seqNow + itemWeight;
        ackedSeq.lazySet(senderId, seqToBe);
        return seqToBe;
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
