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
import static com.hazelcast.util.ExceptionUtil.rethrow;

class SenderTasklet implements Tasklet {

    private static final int PACKET_SIZE_LIMIT = 1 << 14;

    private final Connection connection;
    private final byte[] headerBytes;
    private final Queue<Object> inbox = new ArrayDeque<>();
    private final InboundEdgeStream inboundEdgeStream;
    private final BufferObjectDataOutput outputBuffer;

    // read and written by Jet thread
    private long sentSeq;

    // Written by HZ networking thread, read by Jet thread
    private volatile int sendSeqLimitCompressed;

    SenderTasklet(InboundEdgeStream inboundEdgeStream, NodeEngine nodeEngine, String jetEngineName,
                  Address destinationAddress, long executionId, int destinationVertexId
    ) {
        this.inboundEdgeStream = inboundEdgeStream;
        this.connection = getMemberConnection(nodeEngine, destinationAddress);
        this.headerBytes = createStreamPacketHeader(
                nodeEngine, jetEngineName, executionId, destinationVertexId, inboundEdgeStream.ordinal());
        this.outputBuffer = createObjectDataOutput(nodeEngine);
    }

    @Nonnull
    @Override
    public ProgressState call() {
        ProgressState progressState = inboundEdgeStream.drainTo(inbox);
        if (progressState.isDone()) {
            inbox.add(new ObjectWithPartitionId(DONE_ITEM, -1));
        }
        if (!progressState.isMadeProgress()) {
            return progressState;
        }
        do {
            fillBuffer();
            Packet packet = new Packet(outputBuffer.toByteArray()).setPacketType(Packet.Type.JET);
            connection.write(packet);
            outputBuffer.clear();
        } while (!inbox.isEmpty());

        return progressState;
    }

    private void fillBuffer() {
        try {
            outputBuffer.write(headerBytes);
            // length will be filled in later
            outputBuffer.writeInt(0);
            int writtenCount = 0;
            for (Object item;
                 outputBuffer.position() < PACKET_SIZE_LIMIT
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
            outputBuffer.writeInt(headerBytes.length, writtenCount);
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
