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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.jet.RestartableException;
import com.hazelcast.jet.config.InstanceConfig;
import com.hazelcast.jet.core.metrics.MetricNames;
import com.hazelcast.jet.core.metrics.MetricTags;
import com.hazelcast.jet.impl.util.ObjectWithPartitionId;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;

import static com.hazelcast.jet.impl.Networking.PACKET_HEADER_SIZE;
import static com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFinest;
import static com.hazelcast.jet.impl.util.PrefixedLogger.prefixedLogger;
import static java.lang.Math.ceil;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Receives from a remote member the data associated with a single edge.
 */
public class ReceiverTasklet implements Tasklet {

    /**
     * The {@code ackedSeq} field holds the sequence number acknowledged to the
     * sender as having been received and processed. The sequence increments in
     * terms of the estimated heap occupancy of each received item, in bytes.
     * However, to save on network traffic, the number reported to the sender
     * is coarser-grained: it counts in units of {@code 1 <<
     * COMPRESSED_SEQ_UNIT_LOG2}. For example, with a value of 20 the unit
     * would be one megabyte. The coarse-grained seq is called "compressed
     * seq".
     */
    static final int COMPRESSED_SEQ_UNIT_LOG2 = 16;
    /**
     * The Receive Window, in analogy to TCP's RWIN, is the number of compressed
     * seq units the sender can be ahead of the acknowledged seq. The
     * correspondence between a compressed seq unit and bytes is defined by the
     * constant {@link #COMPRESSED_SEQ_UNIT_LOG2}.
     * <p>
     * This constant specifies the initial size of the receive window. The
     * window is constantly adapted according to the actual data flow through
     * the receiver tasklet.
     */
    static final int INITIAL_RECEIVE_WINDOW_COMPRESSED = 800;

    /**
     * The Receive Window converges towards the amount of data processed per
     * flow-control period multiplied by this number.
     */
    private final int rwinMultiplier;
    private final double flowControlPeriodNs;
    private final ILogger logger;

    /* Used for metrics */
    private final String sourceAddressString;
    private final String ordinalString;
    private final String destinationVertexName;
    private final Connection memberConnection;

    private Queue<byte[]> incoming;
    private final ProgressTracker tracker = new ProgressTracker();
    private final ArrayDeque<ObjWithPtionIdAndSize> inbox = new ArrayDeque<>();
    private final OutboundCollector collector;
    private final InternalSerializationService serializationService;

    private boolean receptionDone;

    @Probe(name = MetricNames.DISTRIBUTED_ITEMS_IN)
    private final Counter itemsInCounter = SwCounter.newSwCounter();

    @Probe(name = MetricNames.DISTRIBUTED_BYTES_IN, unit = ProbeUnit.BYTES)
    private final Counter bytesInCounter = SwCounter.newSwCounter();

    //                    FLOW-CONTROL STATE
    //            All arrays are indexed by sender ID.

    // read by a task scheduler thread, written by a tasklet execution thread
    private volatile long ackedSeq;
    private volatile int numWaitingInInbox;
    private volatile boolean connectionChanged;

    // read and written by updateAndGetSendSeqLimitCompressed(), which is invoked sequentially by a task scheduler
    private int receiveWindowCompressed;
    private int prevAckedSeqCompressed;
    private long prevTimestamp;

    //                 END FLOW-CONTROL STATE

    public ReceiverTasklet(
            OutboundCollector collector, InternalSerializationService serializationService,
            int rwinMultiplier, int flowControlPeriodMs, LoggingService loggingService,
            Address sourceAddress, int ordinal, String destinationVertexName,
            Connection memberConnection, String jobPrefix
    ) {
        this.collector = collector;
        this.serializationService = serializationService;
        this.rwinMultiplier = rwinMultiplier;
        this.flowControlPeriodNs = (double) MILLISECONDS.toNanos(flowControlPeriodMs);
        this.sourceAddressString = sourceAddress.toString();
        this.ordinalString = "" + ordinal;
        this.destinationVertexName = destinationVertexName;
        this.memberConnection = memberConnection;
        String prefix = new StringBuilder()
                .append(jobPrefix)
                .append("/receiverFor:")
                .append(destinationVertexName)
                .append("#")
                .append(ordinal)
                .toString();
        this.logger = prefixedLogger(loggingService.getLogger(getClass()), prefix);
        this.receiveWindowCompressed = INITIAL_RECEIVE_WINDOW_COMPRESSED;
    }

    @Override
    @Nonnull
    public ProgressState call() {
        if (receptionDone) {
            return collector.offerBroadcast(DONE_ITEM);
        }
        if (connectionChanged) {
            throw new RestartableException("The member was reconnected: " + sourceAddressString);
        }
        tracker.reset();
        tracker.notDone();
        tryFillInbox();
        int ackItemLocal = 0;
        for (ObjWithPtionIdAndSize o; (o = inbox.peek()) != null; ) {
            final Object item = o.getItem();
            if (item == DONE_ITEM) {
                receptionDone = true;
                inbox.remove();
                assert inbox.peek() == null : "Found something in the queue beyond the DONE_ITEM: " + inbox.remove();
                break;
            }
            ProgressState outcome = item instanceof BroadcastItem
                    ? collector.offerBroadcast((BroadcastItem) item)
                    : collector.offer(item, o.getPartitionId());
            if (!outcome.isDone()) {
                tracker.madeProgress(outcome.isMadeProgress());
                break;
            }
            tracker.madeProgress();
            inbox.remove();
            ackItemLocal += o.estimatedMemoryFootprint;
        }
        ackItem(ackItemLocal);
        numWaitingInInbox = inbox.size();
        return tracker.toProgressState();
    }

    /**
     * Calls {@link #updateAndGetSendSeqLimitCompressed(long, Connection)} with {@code
     * System.nanoTime()} and the current acked seq for the given sender ID.
     */
    public int updateAndGetSendSeqLimitCompressed(Connection expectedConnection) {
        return updateAndGetSendSeqLimitCompressed(System.nanoTime(), expectedConnection);
    }

    /**
     * Calculates the upper limit for the compressed value of {@link
     * SenderTasklet#sentSeq}, which constrains how much more data the remote
     * sender tasklet can send to this tasklet. Steps to calculate the limit:
     * <ol><li>
     *     Calculate the following:
     *     <ol type="a"><li>
     *         {@code timeDelta} = difference between the timestamps of this and previous
     *         method call
     *     </li><li>
     *         {@code seqDelta} = amount of data processed by the receiver between the calls,
     *         measured in compressed seq units (see {@link #COMPRESSED_SEQ_UNIT_LOG2})
     *     </li><li>
     *         {@code seqsPerAckPeriod = (seqDelta / timeDelta) * }
     *         {@link InstanceConfig#setFlowControlPeriodMs(int)
     *         flowControlPeriodMs}, projected amount of data processed by the receiver
     *         in one standard flow control period (called "ack period" for short)
     *     </li></ol>
     * </li><li>
     *     Define the <emph>target receive window</emph> as {@code 3 * seqsPerAckPeriod}.
     * </li><li>
     *     Adjust the current receive window halfway toward the target receive window.
     * </li><li>
     *     Return the {@code sentSeq} limit as the current acked seq plus the current
     *     receive window.
     * </li></ol>
     *
     * @param timestampNow       value of the timestamp at the time the method is called. The timestamp
     *                           must be obtained from {@code System.nanoTime()}.
     * @param expectedConnection The connection to which the result will be sent. We use it
     *                           to check that it's the same connection the tasklet was crated with.
     */
    // Invoked sequentially by a task scheduler
    int updateAndGetSendSeqLimitCompressed(long timestampNow, Connection expectedConnection) {
        if (!Objects.equals(expectedConnection, memberConnection)) {
            connectionChanged = true;
        }
        final boolean hadPrevStats = prevTimestamp != 0 || prevAckedSeqCompressed != 0;

        final long ackTimeDelta = timestampNow - prevTimestamp;
        prevTimestamp = timestampNow;

        final int ackedSeqCompressed = compressSeq(ackedSeq);
        final int ackedSeqCompressedDelta = ackedSeqCompressed - prevAckedSeqCompressed;
        prevAckedSeqCompressed = ackedSeqCompressed;

        if (hadPrevStats) {
            final double ackedSeqsPerAckPeriod = flowControlPeriodNs * ackedSeqCompressedDelta / ackTimeDelta;
            final int targetRwin = rwinMultiplier * (int) ceil(ackedSeqsPerAckPeriod);
            int rwinDiff = targetRwin - receiveWindowCompressed;
            int numWaitingInInbox = this.numWaitingInInbox;
            // If nothing is waiting in the inbox, our processing speed isn't the cause
            // for less traffic through the processor, it's the sender who's not
            // sending enough data. Don't shrink the RWIN in this case.
            if (numWaitingInInbox == 0 && rwinDiff < 0) {
                rwinDiff = 0;
            }
            rwinDiff /= 2;
            receiveWindowCompressed += rwinDiff;
            if (rwinDiff != 0) {
                logFinest(logger, "receiveWindowCompressed changed by %d to %d", rwinDiff, receiveWindowCompressed);
            }
        }
        return ackedSeqCompressed + receiveWindowCompressed;
    }

    // Only one thread writes to ackedSeq
    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    long ackItem(long itemWeight) {
        return ackedSeq += itemWeight;
    }

    /**
     * To be called only from testing code.
     */
    void setNumWaitingInInbox(int value) {
        this.numWaitingInInbox = value;
    }

    @Override
    public String toString() {
        return "ReceiverTasklet{" +
                "sourceAddressString='" + sourceAddressString + '\'' +
                ", ordinalString='" + ordinalString + '\'' +
                ", destinationVertexName='" + destinationVertexName + '\'' +
                '}';
    }

    static int compressSeq(long seq) {
        return (int) (seq >> COMPRESSED_SEQ_UNIT_LOG2);
    }

    static long estimatedMemoryFootprint(long itemBlobSize) {
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
            long totalBytes = 0;
            long totalItems = 0;
            for (byte[] payload; (payload = incoming.poll()) != null; ) {
                BufferObjectDataInput input = serializationService.createObjectDataInput(payload, PACKET_HEADER_SIZE);
                final int itemCount = input.readInt();
                for (int i = 0; i < itemCount; i++) {
                    final int mark = input.position();
                    final Object item = input.readObject();
                    final int itemSize = input.position() - mark;
                    int partitionId = input.readInt();
                    inbox.add(new ObjWithPtionIdAndSize(item, partitionId, itemSize));
                }
                totalItems += itemCount;
                totalBytes += input.position();
                tracker.madeProgress();
            }
            bytesInCounter.inc(totalBytes);
            itemsInCounter.inc(totalItems);
        } catch (IOException e) {
            throw rethrow(e);
        }
    }

    public void initIncomingQueue(Queue<byte[]> incomingQueue) {
        incoming = incomingQueue;
    }

    private static class ObjWithPtionIdAndSize extends ObjectWithPartitionId {
        final long estimatedMemoryFootprint;

        ObjWithPtionIdAndSize(Object item, int partitionId, int itemBlobSize) {
            super(item, partitionId);
            this.estimatedMemoryFootprint = estimatedMemoryFootprint(itemBlobSize);
        }
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        descriptor = descriptor.withTag(MetricTags.VERTEX, destinationVertexName)
                .withTag(MetricTags.SOURCE_ADDRESS, sourceAddressString)
                .withTag(MetricTags.ORDINAL, ordinalString);

        context.collect(descriptor, this);
    }
}
