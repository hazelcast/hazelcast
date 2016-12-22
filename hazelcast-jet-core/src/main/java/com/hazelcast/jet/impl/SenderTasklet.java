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

import com.hazelcast.nio.Address;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.NodeEngine;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;

import static com.hazelcast.jet.impl.DoneItem.DONE_ITEM;
import static com.hazelcast.jet.impl.JetService.createStreamPacketHeader;
import static com.hazelcast.jet.impl.ReceiverTasklet.compressSeq;
import static com.hazelcast.jet.impl.ReceiverTasklet.estimatedMemoryFootprint;
import static com.hazelcast.jet.impl.Util.createObjectDataOutput;
import static com.hazelcast.jet.impl.Util.getMemberConnection;
import static com.hazelcast.jet.impl.Util.uncheckRun;
import static com.hazelcast.util.ExceptionUtil.rethrow;

class SenderTasklet implements Tasklet {

    private final Connection connection;
    private final Queue<Object> inbox = new ArrayDeque<>();
    private final ProgressTracker progTracker = new ProgressTracker();
    private final InboundEdgeStream inboundEdgeStream;
    private final BufferObjectDataOutput outputBuffer;
    private final int bufPosPastHeader;
    private final int packetSizeLimit;

    private boolean instreamExhausted;
    // read and written by Jet thread
    private long sentSeq;

    // Written by HZ networking thread, read by Jet thread
    private volatile int sendSeqLimitCompressed;

    SenderTasklet(InboundEdgeStream inboundEdgeStream, NodeEngine nodeEngine, String jetEngineName,
                  Address destinationAddress, long executionId, int destinationVertexId, int packetSizeLimit) {
        this.inboundEdgeStream = inboundEdgeStream;
        this.packetSizeLimit = packetSizeLimit;
        this.connection = getMemberConnection(nodeEngine, destinationAddress);
        this.outputBuffer = createObjectDataOutput(nodeEngine);
        uncheckRun(() -> outputBuffer.write(createStreamPacketHeader(
                nodeEngine, jetEngineName, executionId, destinationVertexId, inboundEdgeStream.ordinal())));
        bufPosPastHeader = outputBuffer.position();
    }

    @Nonnull
    @Override
    public ProgressState call() {
        progTracker.reset();
        tryFillInbox();
        if (progTracker.isDone()) {
            return progTracker.toProgressState();
        }
        if (tryFillOutputBuffer()) {
            progTracker.madeProgress();
            connection.write(new Packet(outputBuffer.toByteArray()).setPacketType(Packet.Type.JET));
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
        final ProgressState result = inboundEdgeStream.drainTo(inbox);
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
                ObjectWithPartitionId itemWithpId = (ObjectWithPartitionId) item;
                final int mark = outputBuffer.position();
                outputBuffer.writeObject(itemWithpId.getItem());
                sentSeq += estimatedMemoryFootprint(outputBuffer.position() - mark);
                outputBuffer.writeInt(itemWithpId.getPartitionId());
            }
            outputBuffer.writeInt(bufPosPastHeader, writtenCount);
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
    void setSendSeqLimitCompressed(int sendSeqLimitCompressed) {
        this.sendSeqLimitCompressed = sendSeqLimitCompressed;
    }

    /**
     * Given an uncompressed {@code sentSeq} and a compressed {@code sendSeqLimit}, tells
     * whether the {@code sentSeq} is within the limit specified by the compressed seq.
     */
    // The operations and types in this method must be carefully chosen to properly
    // handle wrap-around that is allowed to happen on sendSeqLimitCompressed.
    static boolean isWithinLimit(long sentSeq, int sendSeqLimitCompressed) {
        return compressSeq(sentSeq) - sendSeqLimitCompressed <= 0;
    }
}
