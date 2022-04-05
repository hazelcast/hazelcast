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
import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.jet.RestartableException;
import com.hazelcast.jet.core.metrics.MetricNames;
import com.hazelcast.jet.core.metrics.MetricTags;
import com.hazelcast.jet.impl.util.ObjectWithPartitionId;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.spi.impl.NodeEngine;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.function.Predicate;

import static com.hazelcast.jet.impl.Networking.createStreamPacketHeader;
import static com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM;
import static com.hazelcast.jet.impl.execution.ReceiverTasklet.compressSeq;
import static com.hazelcast.jet.impl.execution.ReceiverTasklet.estimatedMemoryFootprint;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;

/**
 * The tasklet that sends the data associated with a single edge through network.
 */
public class SenderTasklet implements Tasklet {
    private static final int BUFFER_INITIAL_SIZE = 1 << 10;
    private static final int BUFFER_FIRST_GROWTH_SIZE = 1 << 15;

    private final Connection connection;
    private final Queue<Object> inbox = new ArrayDeque<>();
    private final ProgressTracker progTracker = new ProgressTracker();
    private final InboundEdgeStream inboundEdgeStream;
    private final BufferObjectDataOutput outputBuffer;
    private final int bufPosPastHeader;
    private final int packetSizeLimit;

    /* Used for metrics */
    private final String destinationAddressString;
    private final String sourceOrdinalString;
    private final String sourceVertexName;

    @Probe(name = MetricNames.DISTRIBUTED_ITEMS_OUT)
    private final Counter itemsOutCounter = SwCounter.newSwCounter();

    @Probe(name = MetricNames.DISTRIBUTED_BYTES_OUT, unit = ProbeUnit.BYTES)
    private final Counter bytesOutCounter = SwCounter.newSwCounter();

    private boolean instreamExhausted;
    // read and written by Jet thread
    private long sentSeq;

    // Written by HZ networking thread, read by Jet thread
    private volatile int sendSeqLimitCompressed;
    private final Predicate<Object> addToInboxFunction = inbox::add;

    public SenderTasklet(
            InboundEdgeStream inboundEdgeStream,
            NodeEngine nodeEngine,
            Address destinationAddress,
            Connection connection,
            int destinationVertexId, int packetSizeLimit, long executionId,
            String sourceVertexName, int sourceOrdinal,
            InternalSerializationService serializationService
    ) {
        this.inboundEdgeStream = inboundEdgeStream;
        this.destinationAddressString = destinationAddress.toString();
        this.sourceVertexName = sourceVertexName;
        this.sourceOrdinalString = "" + sourceOrdinal;
        this.packetSizeLimit = packetSizeLimit;
        // we use Connection directly because we rely on packets not being transparently skipped or reordered
        this.connection = connection;
        this.outputBuffer = serializationService.createObjectDataOutput(BUFFER_INITIAL_SIZE, BUFFER_FIRST_GROWTH_SIZE);
        uncheckRun(() -> outputBuffer.write(createStreamPacketHeader(nodeEngine,
                executionId, destinationVertexId, inboundEdgeStream.ordinal())));
        bufPosPastHeader = outputBuffer.position();
    }

    @Nonnull @Override
    public ProgressState call() {
        progTracker.reset();
        tryFillInbox();
        if (progTracker.isDone()) {
            return progTracker.toProgressState();
        }
        if (tryFillOutputBuffer()) {
            progTracker.madeProgress();
            if (!connection.write(new Packet(outputBuffer.toByteArray()).setPacketType(Packet.Type.JET))) {
                throw new RestartableException("Connection write failed in " + toString());
            }
        }
        return progTracker.toProgressState();
    }

    private void tryFillInbox() {
        if (!inbox.isEmpty()) {
            progTracker.notDone();
            return;
        }
        if (instreamExhausted) {
            return;
        }
        progTracker.notDone();
        final ProgressState result = inboundEdgeStream.drainTo(addToInboxFunction);
        progTracker.madeProgress(result.isMadeProgress());
        instreamExhausted = result.isDone();
        if (instreamExhausted) {
            inbox.add(new ObjectWithPartitionId(DONE_ITEM, -1));
        }
    }

    private boolean tryFillOutputBuffer() {
        try {
            // header size + slot for writtenCount
            outputBuffer.position(bufPosPastHeader + Bits.INT_SIZE_IN_BYTES);
            int writtenCount = 0;
            for (Object item;
                 outputBuffer.position() < packetSizeLimit
                         && isWithinLimit(sentSeq, sendSeqLimitCompressed)
                         && (item = inbox.poll()) != null;
                 writtenCount++
            ) {
                ObjectWithPartitionId itemWithPId = item instanceof ObjectWithPartitionId ?
                        (ObjectWithPartitionId) item : new ObjectWithPartitionId(item, -1);
                final int mark = outputBuffer.position();
                outputBuffer.writeObject(itemWithPId.getItem());
                sentSeq += estimatedMemoryFootprint(outputBuffer.position() - mark);
                outputBuffer.writeInt(itemWithPId.getPartitionId());
            }
            outputBuffer.writeInt(bufPosPastHeader, writtenCount);
            bytesOutCounter.inc(outputBuffer.position());
            itemsOutCounter.inc(writtenCount);
            return writtenCount > 0;
        } catch (IOException e) {
            throw rethrow(e);
        }
    }

    /**
     * Updates the upper limit on {@link #sentSeq}, which constrains how much more data this tasklet can send.
     *
     * @param sendSeqLimitCompressed the compressed seq read from a flow-control message. The method
     *                               {@link #isWithinLimit(long, int)} derives the limit on the uncompressed
     *                               {@code sentSeq} from the number supplied here.
     */
    // Called from HZ networking thread
    public void setSendSeqLimitCompressed(int sendSeqLimitCompressed) {
        this.sendSeqLimitCompressed = sendSeqLimitCompressed;
    }

    @Override
    public String toString() {
        return "SenderTasklet{" +
                "ordinal=" + inboundEdgeStream.ordinal() +
                ", destinationAddress=" + destinationAddressString +
                ", sourceVertexName='" + sourceVertexName + '\'' +
                '}';
    }

    /**
     * Given an uncompressed {@code sentSeq} and a compressed {@code sendSeqLimitCompressed}, tells
     * whether the {@code sentSeq} is within the limit specified by the compressed seq.
     */
    // The operations and types in this method must be carefully chosen to properly
    // handle wrap-around that is allowed to happen on sendSeqLimitCompressed.
    static boolean isWithinLimit(long sentSeq, int sendSeqLimitCompressed) {
        return compressSeq(sentSeq) - sendSeqLimitCompressed <= 0;
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        descriptor = descriptor.withTag(MetricTags.VERTEX, sourceVertexName)
                               .withTag(MetricTags.ORDINAL, sourceOrdinalString)
                               .withTag(MetricTags.DESTINATION_ADDRESS, destinationAddressString);

        context.collect(descriptor, this);
    }
}
