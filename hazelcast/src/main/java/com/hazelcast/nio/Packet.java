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

import com.hazelcast.nio.serialization.BinaryClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionSerializer;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DefaultData;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.PortableContext;

import java.nio.ByteBuffer;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;

/**
 * A Packet is a piece of data send over the line.
 */
public final class Packet implements SocketWritable, SocketReadable {

    public static final byte VERSION = 3;

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
    private static final short PERSIST_TYPE = 4;
    private static final short PERSIST_HASH = 5;
    private static final short PERSIST_SIZE = 6;
    private static final short PERSIST_VALUE = 7;

    private static final short PERSIST_COMPLETED = Short.MAX_VALUE;

    private Data data;
    private PortableContext context;
    private short header;
    private int partitionId;
    private transient Connection conn;

    // These 2 fields are only used during read/write. Otherwise they have no meaning.
    private int valueOffset;
    private int valueSize;
    // Stores the current 'phase' of read/write. This is needed so that repeated calls can be made to read/write.
    private short persistStatus;

    private ClassDefinitionSerializer classDefinitionSerializer;

    public Packet(PortableContext context) {
        this.context = context;
    }

    public Packet(Data data, PortableContext context) {
        this(data, -1, context);
    }

    public Packet(Data data, int partitionId, PortableContext context) {
        this.data = data;
        this.context = context;
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

        if (!writeType(destination)) {
            return false;
        }

        if (!writeClassDefinition(destination)) {
            return false;
        }

        if (!writeHash(destination)) {
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

        if (data == null) {
            data = new DefaultData();
        }

        if (!readType(source)) {
            return false;
        }

        if (!readClassDefinition(source)) {
            return false;
        }

        if (!readHash(source)) {
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

    // ========================= type =================================================

    private boolean readType(ByteBuffer source) {
        if (!isPersistStatusSet(PERSIST_TYPE)) {
            if (source.remaining() < INT_SIZE_IN_BYTES + 1) {
                return false;
            }
            int type = source.getInt();
            ((DefaultData) data).setType(type);
            setPersistStatus(PERSIST_TYPE);

            boolean hasClassDefinition = source.get() != 0;
            if (hasClassDefinition) {
                classDefinitionSerializer = new ClassDefinitionSerializer(data, context);
            }
        }
        return true;
    }

    private boolean writeType(ByteBuffer destination) {
        if (!isPersistStatusSet(PERSIST_TYPE)) {
            if (destination.remaining() < INT_SIZE_IN_BYTES + 1) {
                return false;
            }
            int type = data.getType();
            destination.putInt(type);

            boolean hasClassDefinition = context.hasClassDefinition(data);
            destination.put((byte) (hasClassDefinition ? 1 : 0));

            if (hasClassDefinition) {
                classDefinitionSerializer = new ClassDefinitionSerializer(data, context);
            }

            setPersistStatus(PERSIST_TYPE);
        }
        return true;
    }

    // ========================= class definition =================================================

    private boolean readClassDefinition(ByteBuffer source) {
        if (classDefinitionSerializer != null) {
            if (!classDefinitionSerializer.read(source)) {
                return false;
            }
        }
        return true;
    }

    private boolean writeClassDefinition(ByteBuffer destination) {
        if (classDefinitionSerializer != null) {
            if (!classDefinitionSerializer.write(destination)) {
                return false;
            }
        }
        return true;
    }

    // ========================= hash =================================================

    private boolean readHash(ByteBuffer source) {
        if (!isPersistStatusSet(PERSIST_HASH)) {
            if (source.remaining() < INT_SIZE_IN_BYTES) {
                return false;
            }
            ((DefaultData) data).setPartitionHash(source.getInt());
            setPersistStatus(PERSIST_HASH);
        }
        return true;
    }

    private boolean writeHash(ByteBuffer destination) {
        if (!isPersistStatusSet(PERSIST_HASH)) {
            if (destination.remaining() < INT_SIZE_IN_BYTES) {
                return false;
            }
            destination.putInt(data.hasPartitionHash() ? data.getPartitionHash() : 0);
            setPersistStatus(PERSIST_HASH);
        }
        return true;
    }

    // ========================= size =================================================

    private boolean readSize(ByteBuffer source) {
        if (!isPersistStatusSet(PERSIST_SIZE)) {
            if (source.remaining() < INT_SIZE_IN_BYTES) {
                return false;
            }
            valueSize = source.getInt();
            setPersistStatus(PERSIST_SIZE);
        }
        return true;
    }

    private boolean writeSize(ByteBuffer destination) {
        if (!isPersistStatusSet(PERSIST_SIZE)) {
            if (destination.remaining() < INT_SIZE_IN_BYTES) {
                return false;
            }
            valueSize = data.dataSize();
            destination.putInt(valueSize);
            setPersistStatus(PERSIST_SIZE);
        }
        return true;
    }

    // ========================= value =================================================

    private boolean writeValue(ByteBuffer destination) {
        if (!isPersistStatusSet(PERSIST_VALUE)) {
            if (valueSize > 0) {
                // the number of bytes that can be written to the bb.
                int bytesWritable = destination.remaining();

                // the number of bytes that need to be written.
                int bytesNeeded = valueSize - valueOffset;

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
            byte[] bytes = data.getData();
            if (bytes == null) {
                bytes = new byte[valueSize];
                ((DefaultData) data).setData(bytes);
            }

            if (valueSize > 0) {
                int bytesReadable = source.remaining();

                int bytesNeeded = valueSize - valueOffset;

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
        return (data != null ? getDataSize(data, context) : 0) + 7;
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
        classDefinitionSerializer = null;
    }

    private void setPersistStatus(short persistStatus) {
        this.persistStatus = persistStatus;
    }

    private boolean isPersistStatusSet(short status) {
        return this.persistStatus >= status;
    }

    public static int getDataSize(Data data, PortableContext context) {
        // type
        int total = INT_SIZE_IN_BYTES;
        // class def flag
        total += 1;

        if (context.hasClassDefinition(data)) {
            ClassDefinition[] classDefinitions = context.getClassDefinitions(data);
            if (classDefinitions == null || classDefinitions.length == 0) {
                throw new HazelcastSerializationException("ClassDefinition could not be found!");
            }
            // class definitions count
            total += INT_SIZE_IN_BYTES;

            for (ClassDefinition classDef : classDefinitions) {
                // classDefinition-classId
                total += INT_SIZE_IN_BYTES;
                // classDefinition-factory-id
                total += INT_SIZE_IN_BYTES;
                // classDefinition-version
                total += INT_SIZE_IN_BYTES;
                // classDefinition-binary-length
                total += INT_SIZE_IN_BYTES;
                byte[] bytes = ((BinaryClassDefinition) classDef).getBinary();
                // classDefinition-binary
                total += bytes.length;
            }
        }

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
