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
import com.hazelcast.nio.Bits;
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

    public static final int BUFFER_SIZE = 16 * 1024;
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
        this.headerBytes = getHeaderBytes(engineName, executionId, destinationVertexId, inboundEdgeStream.ordinal());
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
        if (progressState.isMadeProgress()) {
            try {
                outputBuffer.write(headerBytes);
                outputBuffer.writeInt(inbox.size());
                for (Object item; (item = inbox.poll()) != null; ) {
                    ObjectWithPartitionId itemWithpId = (ObjectWithPartitionId) item;
                    outputBuffer.writeObject(itemWithpId.getItem());
                    outputBuffer.writeInt(itemWithpId.getPartitionId());

                }
            } catch (IOException e) {
                throw rethrow(e);
            }
            Packet packet = new Packet(outputBuffer.toByteArray()).setFlag(Packet.FLAG_JET);
            connection.write(packet);
            outputBuffer.clear();
        }
        return progressState;
    }

    private static byte[] getHeaderBytes(String name, long executionId, int destinationVertexId, int ordinal) {
        byte[] nameBytes = name.getBytes(JetService.CHARSET);
        int length = Bits.INT_SIZE_IN_BYTES + nameBytes.length + Bits.LONG_SIZE_IN_BYTES + Bits.INT_SIZE_IN_BYTES
                + Bits.INT_SIZE_IN_BYTES;
        byte[] headerBytes = new byte[length];
        int offset = 0;
        Bits.writeIntB(headerBytes, offset, nameBytes.length);
        offset += Bits.INT_SIZE_IN_BYTES;
        System.arraycopy(nameBytes, 0, headerBytes, offset, nameBytes.length);
        offset += nameBytes.length;
        Bits.writeLongB(headerBytes, offset, executionId);
        offset += Bits.LONG_SIZE_IN_BYTES;
        Bits.writeIntB(headerBytes, offset, destinationVertexId);
        offset += Bits.INT_SIZE_IN_BYTES;
        Bits.writeIntB(headerBytes, offset, ordinal);
        return headerBytes;
    }
}
