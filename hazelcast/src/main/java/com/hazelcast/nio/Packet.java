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

package com.hazelcast.nio;

import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.spi.annotation.PrivateApi;

import java.nio.ByteBuffer;

import static com.hazelcast.nio.Bits.BYTE_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.SHORT_SIZE_IN_BYTES;

/**
 * A Packet is a piece of data send over the line. The Packet is used for member to member communication.
 *
 * The Packet extends HeapData instead of wrapping it. From a design point of view this is often not the preferred solution (
 * prefer composition over inheritance), but in this case that would mean more object litter.
 *
 * Since the Packet isn't used throughout the system, this design choice is visible locally.
 */
@PrivateApi
public final class Packet extends HeapData implements OutboundFrame {

    public static final byte VERSION = 4;

    public static final int FLAG_OP = 1 << 0;
    public static final int FLAG_RESPONSE = 1 << 1;
    public static final int FLAG_EVENT = 1 << 2;
    public static final int FLAG_URGENT = 1 << 4;
    public static final int FLAG_BIND = 1 << 5;

    /**
     * A flag to indicate this is a special control packet for the operation system like invocation-heartbeats.
     */
    public static final int FLAG_OP_CONTROL = 1 << 6;

    private static final int HEADER_SIZE = BYTE_SIZE_IN_BYTES + SHORT_SIZE_IN_BYTES + INT_SIZE_IN_BYTES + INT_SIZE_IN_BYTES;

    private short flags;
    private int partitionId;
    private transient Connection conn;

    // These 2 fields are only used during read/write. Otherwise they have no meaning.
    private int valueOffset;
    private int size;
    // Stores the current 'phase' of read/write. This is needed so that repeated calls can be made to read/write.
    private boolean headerComplete;

    public Packet() {
    }

    public Packet(byte[] payload) {
        this(payload, -1);
    }

    public Packet(byte[] payload, int partitionId) {
        super(payload);
        this.partitionId = partitionId;
    }

    /**
     * Gets the Connection this Packet was send with.
     *
     * @return the Connection. Could be null.
     */
    public Connection getConn() {
        return conn;
    }

    /**
     * Sets the Connection this Packet is send with.
     * <p/>
     * This is done on the reading side of the Packet to make it possible to retrieve information about
     * the sender of the Packet.
     *
     * @param conn the connection.
     */
    public void setConn(Connection conn) {
        this.conn = conn;
    }

    /**
     * Sets a particular flag. The other flags will not be touched.
     *
     * @param flag the flag to set
     * @return this (for fluent interface)
     */
    public Packet setFlag(int flag) {
        flags = (short) (flags | flag);
        return this;
    }

    /**
     * Sets all flags at once. The old flags will be completely overwritten by the new flags.
     *
     * The reason this method accepts an int instead of a short is that Java immediately converts to ints,
     * so you would have to do bit shifting logic to down cast to a short all the time.*
     *
     * @param flags the flags.
     * @return this (for fluent interface)
     */
    public Packet setAllFlags(int flags) {
        this.flags = (short) flags;
        return this;
    }

    /**
     * Checks if a flag is set.
     *
     * @param flag the flag to check
     * @return true if the flag is set, false otherwise.
     */
    public boolean isFlagSet(int flag) {
        return (flags & flag) != 0;
    }

    /**
     * Returns the flags of the Packet. The flags is used to figure out what the content is of this Packet before
     * the actual payload needs to be processed.
     *
     * @return the flags.
     */
    public short getFlags() {
        return flags;
    }

    /**
     * Returns the partition id of this packet. If this packet is not for a particular partition, -1 is returned.
     *
     * @return the partition id.
     */
    public int getPartitionId() {
        return partitionId;
    }

    public void reset() {
        headerComplete = false;
    }

    @Override
    public boolean isUrgent() {
        return isFlagSet(FLAG_URGENT);
    }

    public boolean writeTo(ByteBuffer dst) {
        if (!headerComplete) {
            if (dst.remaining() < HEADER_SIZE) {
                return false;
            }

            dst.put(VERSION);
            dst.putShort(flags);
            dst.putInt(partitionId);
            size = totalSize();
            dst.putInt(size);
            headerComplete = true;
        }

        return writeValue(dst);
    }

    public boolean readFrom(ByteBuffer src) {
        if (!headerComplete) {
            if (src.remaining() < HEADER_SIZE) {
                return false;
            }

            byte version = src.get();
            if (VERSION != version) {
                throw new IllegalArgumentException("Packet versions are not matching! Expected -> "
                        + VERSION + ", Incoming -> " + version);
            }

            flags = src.getShort();
            partitionId = src.getInt();
            size = src.getInt();
            headerComplete = true;
        }

        return readValue(src);
    }

    // ========================= value =================================================

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

    private boolean writeValue(ByteBuffer dst) {
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
                // Not all bytes for the value are available. So lets write as much as is available.
                bytesWrite = bytesWritable;
                done = false;
            }

            byte[] byteArray = toByteArray();
            dst.put(byteArray, valueOffset, bytesWrite);
            valueOffset += bytesWrite;

            if (!done) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns an estimation of the packet, including its payload, in bytes.
     *
     * @return the size of the packet.
     */
    public int packetSize() {
        return (payload != null ? totalSize() : 0) + HEADER_SIZE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof Packet)) {
            return false;
        }

        Packet packet = (Packet) o;
        if (!super.equals(packet)) {
            return false;
        }

        if (flags != packet.flags) {
            return false;
        }
        return partitionId == packet.partitionId;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (int) flags;
        result = 31 * result + partitionId;
        return result;
    }

    @Override
    public String toString() {
        return "Packet{"
                + "flags=" + flags
                + ", isResponse=" + isFlagSet(Packet.FLAG_RESPONSE)
                + ", isOperation=" + isFlagSet(Packet.FLAG_OP)
                + ", isEvent=" + isFlagSet(Packet.FLAG_EVENT)
                + ", partitionId=" + partitionId
                + ", conn=" + conn
                + '}';
    }
}
