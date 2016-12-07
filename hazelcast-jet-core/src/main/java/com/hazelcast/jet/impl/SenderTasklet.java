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
import static com.hazelcast.jet.impl.Util.createObjectDataOutput;
import static com.hazelcast.jet.impl.Util.getMemberConnection;
import static com.hazelcast.util.ExceptionUtil.rethrow;

class SenderTasklet implements Tasklet {

    public static final int PACKET_SIZE_LIMIT = 1 << 14;
    private static final int RECEIVE_WINDOW = 1 << 23;

    private final Connection connection;
    private final byte[] headerBytes;
    private final Queue<Object> inbox = new ArrayDeque<>();
    private final InboundEdgeStream inboundEdgeStream;
    private final BufferObjectDataOutput outputBuffer;

    private int sentSeq;
    private volatile int ackedSeq;

    SenderTasklet(InboundEdgeStream inboundEdgeStream, NodeEngine nodeEngine, String jetEngineName,
                         Address destinationAddress, long executionId, int destinationVertexId
    ) {
        this.inboundEdgeStream = inboundEdgeStream;
        this.connection = getMemberConnection(nodeEngine, destinationAddress);
        this.headerBytes = createStreamPacketHeader(
                nodeEngine, jetEngineName, executionId, destinationVertexId, inboundEdgeStream.ordinal());
        this.outputBuffer = createObjectDataOutput(nodeEngine);
    }

    void setAckedSeq(int ackedSeq) {
        this.ackedSeq = ackedSeq;
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
                     && isWithinWindow()
                     && (item = inbox.poll()) != null;
                 writtenCount++, sentSeq++
            ) {
                ObjectWithPartitionId itemWithpId = (ObjectWithPartitionId) item;
                outputBuffer.writeObject(itemWithpId.getItem());
                outputBuffer.writeInt(itemWithpId.getPartitionId());
            }
            outputBuffer.writeInt(headerBytes.length, writtenCount);
        } catch (IOException e) {
            throw rethrow(e);
        }
    }

    private boolean isWithinWindow() {
        final long ackedSeq = this.ackedSeq;
        long sentSeq = this.sentSeq;
        if (sentSeq < 0 && ackedSeq > 0) {
            // handle integer overflow case where sentSeq has wrapped around, but ackedSeq hasn't yet
            sentSeq = Integer.toUnsignedLong(this.sentSeq);
        }
        return sentSeq < ackedSeq + RECEIVE_WINDOW;
    }
}
