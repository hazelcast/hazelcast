/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DefaultData;

import java.nio.ByteBuffer;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;

/**
 * A Packet is a piece of data send over the line.
 */
public final class Packet implements SocketWritable, SocketReadable {

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

    private Data data;
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

    public Packet(Data data) {
        this(data, -1);
    }

    public Packet(Data data, int partitionId) {
        this.data = data;
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
    public boolean writeTo(ByteBuffer destination) {
        if (!writeVersion(destination)) {
            return false;
        }

        if (!writeHeader(destination)) {
            return false;
        }

        if (!writePartition(destination)) {
            return false;
        }

        if (!writeSize(destination)) {
            return false;
        }

        if (!writeValue(destination)) {
            return false;
        }

        setPersistStatus(PERSIST_COMPLETED);
        return true;
    }

    @Override
    public boolean readFrom(ByteBuffer source) {
        if (!readVersion(source)) {
            return false;
        }

        if (!readHeader(source)) {
            return false;
        }

        if (!readPartition(source)) {
            return false;
        }

        if (!readSize(source)) {
            return false;
        }

        if (!readValue(source)) {
            return false;
        }

        setPersistStatus(PERSIST_COMPLETED);
        return true;
    }

    // ========================= version =================================================

    private boolean readVersion(ByteBuffer source) {
        if (!isPersistStatusSet(PERSIST_VERSION)) {
            if (!source.hasRemaining()) {
                return false;
            }
            byte version = source.get();
            setPersistStatus(PERSIST_VERSION);
            if (VERSION != version) {
                throw new IllegalArgumentException("Packet versions are not matching! Expected -> "
                        + VERSION + ", Incoming -> " + version);
            }
        }
        return true;
    }

    private boolean writeVersion(ByteBuffer destination) {
        if (!isPersistStatusSet(PERSIST_VERSION)) {
            if (!destination.hasRemaining()) {
                return false;
            }
            destination.put(VERSION);
            setPersistStatus(PERSIST_VERSION);
        }
        return true;
    }

    // ========================= header =================================================

    private boolean readHeader(ByteBuffer source) {
        if (!isPersistStatusSet(PERSIST_HEADER)) {
            if (source.remaining() < 2) {
                return false;
            }
            header = source.getShort();
            setPersistStatus(PERSIST_HEADER);
        }
        return true;
    }

    private boolean writeHeader(ByteBuffer destination) {
        if (!isPersistStatusSet(PERSIST_HEADER)) {
            if (destination.remaining() < Bits.SHORT_SIZE_IN_BYTES) {
                return false;
            }
            destination.putShort(header);
            setPersistStatus(PERSIST_HEADER);
        }
        return true;
    }

    // ========================= partition =================================================

    private boolean readPartition(ByteBuffer source) {
        if (!isPersistStatusSet(PERSIST_PARTITION)) {
            if (source.remaining() < 4) {
                return false;
            }
            partitionId = source.getInt();
            setPersistStatus(PERSIST_PARTITION);
        }
        return true;
    }


    private boolean writePartition(ByteBuffer destination) {
        if (!isPersistStatusSet(PERSIST_PARTITION)) {
            if (destination.remaining() < Bits.INT_SIZE_IN_BYTES) {
                return false;
            }
            destination.putInt(partitionId);
            setPersistStatus(PERSIST_PARTITION);
        }
        return true;
    }


    // ========================= size =================================================

    private boolean readSize(ByteBuffer source) {
        if (!isPersistStatusSet(PERSIST_SIZE)) {
            if (source.remaining() < INT_SIZE_IN_BYTES) {
                return false;
            }
            size = source.getInt();
            setPersistStatus(PERSIST_SIZE);
        }
        return true;
    }

    private boolean writeSize(ByteBuffer destination) {
        if (!isPersistStatusSet(PERSIST_SIZE)) {
            if (destination.remaining() < INT_SIZE_IN_BYTES) {
                return false;
            }
            size = data.totalSize();
            destination.putInt(size);
            setPersistStatus(PERSIST_SIZE);
        }
        return true;
    }

    // ========================= value =================================================

    private boolean writeValue(ByteBuffer destination) {
        if (!isPersistStatusSet(PERSIST_VALUE)) {
            if (size > 0) {
                // the number of bytes that can be written to the bb.
                int bytesWritable = destination.remaining();

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

                byte[] byteArray = data.getData();
                destination.put(byteArray, valueOffset, bytesWrite);
                valueOffset += bytesWrite;

                if (!done) {
                    return false;
                }
            }
            setPersistStatus(PERSIST_VALUE);
        }
        return true;
    }

    private boolean readValue(ByteBuffer source) {
        if (!isPersistStatusSet(PERSIST_VALUE)) {
            byte[] bytes;
            if (data == null) {
                bytes = new byte[size];
                data = new DefaultData(bytes);
            } else {
                bytes = data.getData();
            }

            if (size > 0) {
                int bytesReadable = source.remaining();

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
                source.get(bytes, valueOffset, bytesRead);
                valueOffset += bytesRead;

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
    public int size() {
        // 7 = byte(version) + short(header) + int(partitionId)
        return (data != null ? getDataSize(data) : 0) + 7;
    }

    public Data getData() {
        return data;
    }

    public void setData(Data data) {
        this.data = data;
    }

    public boolean done() {
        return isPersistStatusSet(PERSIST_COMPLETED);
    }

    public void reset() {
        data = null;
        persistStatus = 0;
    }

    private void setPersistStatus(short persistStatus) {
        this.persistStatus = persistStatus;
    }

    private boolean isPersistStatusSet(short status) {
        return this.persistStatus >= status;
    }

    private static int getDataSize(Data data) {
        // type
        int total = INT_SIZE_IN_BYTES;
        // class def flag
        total += 1;

        // partition-hash
        total += INT_SIZE_IN_BYTES;
        // data-size
        total += INT_SIZE_IN_BYTES;
        // data
        total += data.dataSize();
        return total;
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
