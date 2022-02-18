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

package com.hazelcast.internal.nio;

import java.nio.ByteBuffer;

import static com.hazelcast.internal.nio.Bits.BYTE_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.SHORT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Packet.VERSION;

/**
 * Responsible for writing or reading a Packet. Originally the logic was placed in the Packet. The problem with this approach
 * is that a single Packet instance can't be shared between multiple connections and this leads to increased memory usage since
 * the packet needs to be copied for every connection.
 *
 * The {@link PacketIOHelper} is stateful because it tracks where the packet reading from ByteBuffer or writing to ByteBuffer.
 *
 * A {@link PacketIOHelper} instance should only be used for reading, or only be used for writing. So if you need to read and
 * write at the same time, you need to have 2 instances.
 *
 * A {@link PacketIOHelper} is designed to be reused.
 */
public class PacketIOHelper {
    static final int HEADER_SIZE = BYTE_SIZE_IN_BYTES + SHORT_SIZE_IN_BYTES + INT_SIZE_IN_BYTES + INT_SIZE_IN_BYTES;

    private int valueOffset;
    private int size;
    private boolean headerComplete;
    private char flags;
    private int partitionId;
    private byte[] payload;

    /**
     * Writes the packet data to the supplied {@code ByteBuffer}, up to the buffer's limit. If it returns {@code false},
     * it should be called again to write the remaining data.
     *
     * @param dst the destination byte buffer
     * @return {@code true} if all the packet's data is now written out; {@code false} otherwise.
     */
    public boolean writeTo(Packet packet, ByteBuffer dst) {
        if (!headerComplete) {
            if (dst.remaining() < HEADER_SIZE) {
                return false;
            }

            dst.put(VERSION);
            dst.putChar(packet.getFlags());
            dst.putInt(packet.getPartitionId());
            size = packet.totalSize();
            dst.putInt(size);
            headerComplete = true;
        }

        if (writeValue(packet, dst)) {
            reset();
            return true;
        } else {
            return false;
        }
    }

    private boolean writeValue(Packet packet, ByteBuffer dst) {
        if (size > 0) {
            // the number of bytes that can be written to the bb.
            int bytesWritable = dst.remaining();

            // the number of bytes that need to be written.
            int bytesNeeded = size - valueOffset;

            int bytesWrite;
            boolean done;
            if (bytesWritable >= bytesNeeded) {
                // All bytes for the value are available.
                bytesWrite = bytesNeeded;
                done = true;
            } else {
                // Not all bytes for the value are available. So let's write as much as is available.
                bytesWrite = bytesWritable;
                done = false;
            }

            byte[] byteArray = packet.toByteArray();
            dst.put(byteArray, valueOffset, bytesWrite);
            valueOffset += bytesWrite;

            if (!done) {
                return false;
            }
        }
        return true;
    }

    /**
     * Reads the packet data from the supplied {@code ByteBuffer}. The buffer may not contain the complete packet.
     * If this method returns {@code false}, it should be called again to read more packet data.
     *
     * @param src the source byte buffer
     * @return the read Packet if all the packet's data is now read; {@code null} otherwise.
     */
    public Packet readFrom(ByteBuffer src) {
        if (!headerComplete) {
            if (src.remaining() < HEADER_SIZE) {
                return null;
            }

            byte version = src.get();
            if (VERSION != version) {
                throw new IllegalArgumentException("Packet versions are not matching! Expected -> "
                        + VERSION + ", Incoming -> " + version);
            }

            flags = src.getChar();
            partitionId = src.getInt();
            size = src.getInt();
            headerComplete = true;
        }

        if (readValue(src)) {
            Packet packet = new Packet(payload, partitionId).resetFlagsTo(flags);
            reset();
            return packet;
        } else {
            return null;
        }
    }

    private void reset() {
        headerComplete = false;
        payload = null;
        valueOffset = 0;
    }

    private boolean readValue(ByteBuffer src) {
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

            // read the data from the byte-buffer into the bytes-array.
            src.get(payload, valueOffset, bytesRead);
            valueOffset += bytesRead;

            if (!done) {
                return false;
            }
        }

        return true;
    }

}
