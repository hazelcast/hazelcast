/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import java.nio.ByteBuffer;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;

/**
 * A Packet is a piece of data send over the line.
 *
 * The Packet extends HeapData instead of wrapping it. From a design point of view this is often not the preferred solution (
 * prefer composition over inheritance), but in this case that would mean more object litter.
 *
 * Since the Packet isn't used throughout the system, this design choice is visible locally.
 */
public final class Packet extends HeapData
        implements SocketWritable, SocketReadable {

    public static final byte VERSION = 4;

    public static final int HEADER_OP = 0;
    public static final int HEADER_RESPONSE = 1;
    public static final int HEADER_EVENT = 2;
    public static final int HEADER_WAN_REPLICATION = 3;
    public static final int HEADER_URGENT = 4;
    public static final int HEADER_BIND = 5;

    // The value of these constants is important. The order needs to match the order in the read/write process
    private static final short PERSIST_VERSION = 1;
    private static final short PERSIST_HEADER = 2;
    private static final short PERSIST_PARTITION = 3;
    private static final short PERSIST_SIZE = 4;
    private static final short PERSIST_VALUE = 5;

    private static final short PERSIST_COMPLETED = Short.MAX_VALUE;

    private short header;
    private int partitionId;
    private transient Connection conn;

    // These 2 fields are only used during read/write. Otherwise they have no meaning.
    private int valueOffset;
    private int size;
    // Stores the current 'phase' of read/write. This is needed so that repeated calls can be made to read/write.
    private short persistStatus;

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

    public void setHeader(int bit) {
        header |= 1 << bit;
    }

    public boolean isHeaderSet(int bit) {
        return (header & 1 << bit) != 0;
    }

    /**
     * Returns the header of the Packet. The header is used to figure out what the content is of this Packet before
     * the actual payload needs to be processed.
     *
     * @return the header.
     */
    public short getHeader() {
        return header;
    }

    /**
     * Returns the partition id of this packet. If this packet is not for a particular partition, -1 is returned.
     *
     * @return the partition id.
     */
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public boolean isUrgent() {
        return isHeaderSet(HEADER_URGENT);
    }

    @Override
    public boolean writeTo(ByteBuffer dst) {
        if (!writeVersion(dst)) {
            return false;
        }

        if (!writeHeader(dst)) {
            return false;
        }

        if (!writePartition(dst)) {
            return false;
        }

        if (!writeSize(dst)) {
            return false;
        }

        if (!writeValue(dst)) {
            return false;
        }

        setPersistStatus(PERSIST_COMPLETED);
        return true;
    }

    @Override
    public boolean readFrom(ByteBuffer src) {
        if (!readVersion(src)) {
            return false;
        }

        if (!readHeader(src)) {
            return false;
        }

        if (!readPartition(src)) {
            return false;
        }

        if (!readSize(src)) {
            return false;
        }

        if (!readValue(src)) {
            return false;
        }

        setPersistStatus(PERSIST_COMPLETED);
        return true;
    }

    // ========================= version =================================================

    private boolean readVersion(ByteBuffer src) {
        if (!isPersistStatusSet(PERSIST_VERSION)) {
            if (!src.hasRemaining()) {
                return false;
            }
            byte version = src.get();
            setPersistStatus(PERSIST_VERSION);
            if (VERSION != version) {
                throw new IllegalArgumentException("Packet versions are not matching! Expected -> "
                        + VERSION + ", Incoming -> " + version);
            }
        }
        return true;
    }

    private boolean writeVersion(ByteBuffer dst) {
        if (!isPersistStatusSet(PERSIST_VERSION)) {
            if (!dst.hasRemaining()) {
                return false;
            }
            dst.put(VERSION);
            setPersistStatus(PERSIST_VERSION);
        }
        return true;
    }

    // ========================= header =================================================

    private boolean readHeader(ByteBuffer src) {
        if (!isPersistStatusSet(PERSIST_HEADER)) {
            if (src.remaining() < 2) {
                return false;
            }
            header = src.getShort();
            setPersistStatus(PERSIST_HEADER);
        }
        return true;
    }

    private boolean writeHeader(ByteBuffer dst) {
        if (!isPersistStatusSet(PERSIST_HEADER)) {
            if (dst.remaining() < Bits.SHORT_SIZE_IN_BYTES) {
                return false;
            }
            dst.putShort(header);
            setPersistStatus(PERSIST_HEADER);
        }
        return true;
    }

    // ========================= partition =================================================

    private boolean readPartition(ByteBuffer src) {
        if (!isPersistStatusSet(PERSIST_PARTITION)) {
            if (src.remaining() < 4) {
                return false;
            }
            partitionId = src.getInt();
            setPersistStatus(PERSIST_PARTITION);
        }
        return true;
    }


    private boolean writePartition(ByteBuffer dst) {
        if (!isPersistStatusSet(PERSIST_PARTITION)) {
            if (dst.remaining() < Bits.INT_SIZE_IN_BYTES) {
                return false;
            }
            dst.putInt(partitionId);
            setPersistStatus(PERSIST_PARTITION);
        }
        return true;
    }

    // ========================= size =================================================

    private boolean readSize(ByteBuffer src) {
        if (!isPersistStatusSet(PERSIST_SIZE)) {
            if (src.remaining() < INT_SIZE_IN_BYTES) {
                return false;
            }
            size = src.getInt();
            setPersistStatus(PERSIST_SIZE);
        }
        return true;
    }

    private boolean writeSize(ByteBuffer dst) {
        if (!isPersistStatusSet(PERSIST_SIZE)) {
            if (dst.remaining() < INT_SIZE_IN_BYTES) {
                return false;
            }
            size = totalSize();
            dst.putInt(size);
            setPersistStatus(PERSIST_SIZE);
        }
        return true;
    }

    // ========================= value =================================================

    private boolean readValue(ByteBuffer src) {
        if (!isPersistStatusSet(PERSIST_VALUE)) {
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

            setPersistStatus(PERSIST_VALUE);
        }
        return true;
    }

    private boolean writeValue(ByteBuffer dst) {
        if (!isPersistStatusSet(PERSIST_VALUE)) {
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
            setPersistStatus(PERSIST_VALUE);
        }
        return true;
    }

    /**
     * Returns an estimation of the packet, including its payload, in bytes.
     *
     * @return the size of the packet.
     */
    public int packetSize() {
        // 11 = byte(version) + short(header) + int(partitionId) + int(data size)
        return (payload != null ? totalSize() : 0) + 11;
    }

    public boolean done() {
        return isPersistStatusSet(PERSIST_COMPLETED);
    }

    public void reset() {
        payload = null;
        persistStatus = 0;
    }

    private void setPersistStatus(short persistStatus) {
        this.persistStatus = persistStatus;
    }

    private boolean isPersistStatusSet(short status) {
        return this.persistStatus >= status;
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

        if (header != packet.header) {
            return false;
        }
        return partitionId == packet.partitionId;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (int) header;
        result = 31 * result + partitionId;
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Packet{");
        sb.append("header=").append(header);
        sb.append(", isResponse=").append(isHeaderSet(Packet.HEADER_RESPONSE));
        sb.append(", isOperation=").append(isHeaderSet(Packet.HEADER_OP));
        sb.append(", isEvent=").append(isHeaderSet(Packet.HEADER_EVENT));
        sb.append(", partitionId=").append(partitionId);
        sb.append(", conn=").append(conn);
        sb.append('}');
        return sb.toString();
    }
}
