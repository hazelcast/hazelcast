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
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import static com.hazelcast.jet2.impl.DoneItem.DONE_ITEM;

class RemoteOutboundCollector implements OutboundCollector {

    private final Connection connection;
    private final byte[] headerBytes;
    private final int[] partitions;
    private final InternalSerializationService serializationService;

    RemoteOutboundCollector(NodeEngine engine,
                                   String engineName,
                                   Address destinationAddress,
                                   long executionId,
                                   int destinationVertexId,
                                   int ordinal,
                                   int[] partitions) {
        this.serializationService = (InternalSerializationService) engine.getSerializationService();
        this.connection = ((NodeEngineImpl) engine).getNode().getConnectionManager().getConnection(destinationAddress);
        this.partitions = partitions;
        this.headerBytes = getHeaderBytes(engineName, executionId, destinationVertexId, ordinal);
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

    @Override
    public ProgressState offer(Object item) {
        return offer(item, -1);
    }

    @Override
    public ProgressState offer(Object item, int partitionId) {
        byte[] buffer = serializationService.toBytes(headerBytes.length, item);
        System.arraycopy(headerBytes, 0, buffer, 0, headerBytes.length);
        connection.write(new Packet(buffer, partitionId).setFlag(Packet.FLAG_JET));
        return ProgressState.DONE;
    }

    @Override
    public ProgressState close() {
        return offer(DONE_ITEM);
    }

    @Override
    public int[] getPartitions() {
        return partitions;
    }

}
