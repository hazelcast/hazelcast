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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.annotation.Nonnull;
import java.io.IOException;

import static com.hazelcast.jet2.impl.DoneItem.DONE_ITEM;
import static com.hazelcast.util.ExceptionUtil.rethrow;

public class SenderTasklet implements Tasklet {

    public static final int BUFFER_SIZE = 1 << 15;
    public static final int PACKET_SIZE_LIMIT = 1 << 14;
    private final Connection connection;
    private final byte[] headerBytes;
    private final ArrayDequeWithObserver inbox = new ArrayDequeWithObserver();
    private final InboundEdgeStream inboundEdgeStream;
    private final BufferObjectDataOutput outputBuffer;

    public SenderTasklet(InboundEdgeStream inboundEdgeStream,
                         NodeEngine engine, String engineName,
                         Address destinationAddress, long executionId,
                         int destinationVertexId) {
        this.inboundEdgeStream = inboundEdgeStream;
        this.connection = ((NodeEngineImpl) engine).getNode().getConnectionManager().getConnection(destinationAddress);
        this.headerBytes = JetService.createHeader(engineName, executionId, destinationVertexId, inboundEdgeStream.ordinal());
        this.outputBuffer = ((InternalSerializationService) engine.getSerializationService())
                .createObjectDataOutput(BUFFER_SIZE);
    }

    @Nonnull
    @Override
    public ProgressState call() {
        ProgressState progressState = inboundEdgeStream.drainTo(inbox);
        if (progressState.isDone()) {
            inbox.offer(new ObjectWithPartitionId(DONE_ITEM, -1));
        }
        if (!progressState.isMadeProgress()) {
            return progressState;
        }
        do {
            fillBuffer();
            Packet packet = new Packet(outputBuffer.toByteArray()).setFlag(Packet.FLAG_JET);
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
            for (Object item; outputBuffer.position() < PACKET_SIZE_LIMIT
                    && (item = inbox.poll()) != null; writtenCount++) {
                ObjectWithPartitionId itemWithpId = (ObjectWithPartitionId) item;
                outputBuffer.writeObject(itemWithpId.getItem());
                outputBuffer.writeInt(itemWithpId.getPartitionId());
            }
            outputBuffer.writeInt(headerBytes.length, writtenCount);
        } catch (IOException e) {
            throw rethrow(e);
        }
    }
}
