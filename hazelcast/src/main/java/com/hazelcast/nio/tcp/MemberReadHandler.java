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

package com.hazelcast.nio.tcp;

import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.packetdispatcher.PacketDispatcher;

import java.nio.ByteBuffer;

import static com.hazelcast.nio.Packet.FLAG_URGENT;

/**
 * The {@link ReadHandler} for member to member communication.
 *
 * It reads as many packets from the src ByteBuffer as possible, and each of the Packets is send to the {@link PacketDispatcher}.
 *
 * @see PacketDispatcher
 * @see MemberWriteHandler
 */
public class MemberReadHandler implements ReadHandler {

    protected final TcpIpConnection connection;

    private final PacketDispatcher packetDispatcher;
    private final Counter normalPacketsRead;
    private final Counter priorityPacketsRead;

    // These 2 fields are only used during read/write. Otherwise they have no meaning.
    private int valueOffset;
    private int size;
    // Stores the current 'phase' of read/write. This is needed so that repeated calls can be made to read/write.
    private boolean headerComplete;
    private byte[] payload;
    private short flags;
    private int partitionId;

    public MemberReadHandler(TcpIpConnection connection, PacketDispatcher packetDispatcher) {
        this.connection = connection;
        this.packetDispatcher = packetDispatcher;
        SocketReader socketReader = connection.getSocketReader();
        this.normalPacketsRead = socketReader.getNormalFramesReadCounter();
        this.priorityPacketsRead = socketReader.getPriorityFramesReadCounter();
    }

    @Override
    public void onRead(ByteBuffer src) throws Exception {
        while (src.hasRemaining()) {
            if (!readPacket(src)) {
                return;
            }

            Packet packet = new Packet(flags, partitionId, payload).setConn(connection);

            if (packet.isFlagSet(FLAG_URGENT)) {
                priorityPacketsRead.inc();
            } else {
                normalPacketsRead.inc();
            }

            packetDispatcher.dispatch(packet);

            // null it to prevent retaining memory
            payload = null;
            headerComplete = false;
            valueOffset = 0;
        }
    }

    /**
     * Reads the content of a packet. If the packet is not fully read, false is returned.
     */
    private boolean readPacket(ByteBuffer src) {
        if (!headerComplete) {
            if (src.remaining() < Packet.HEADER_SIZE) {
                return false;
            }

            byte version = src.get();
            if (Packet.VERSION != version) {
                throw new IllegalArgumentException("Packet versions are not matching! Expected -> "
                        + Packet.VERSION + ", Incoming -> " + version);
            }

            flags = src.getShort();
            partitionId = src.getInt();
            size = src.getInt();
            headerComplete = true;
        }

        if (payload == null) {
            payload = new byte[size];
        }

        if (size > 0) {
            int bytesReadable = src.remaining();

            int bytesNeeded = size - valueOffset;

            boolean done;
            int bytesRead;
            if (bytesReadable >= bytesNeeded) {
                bytesRead = bytesNeeded;
                done = true;
            } else {
                bytesRead = bytesReadable;
                done = false;
            }

            // read the data from the byte-buffer into the payload
            src.get(payload, valueOffset, bytesRead);
            valueOffset += bytesRead;

            if (!done) {
                return false;
            }
        }

        return true;
    }
}
