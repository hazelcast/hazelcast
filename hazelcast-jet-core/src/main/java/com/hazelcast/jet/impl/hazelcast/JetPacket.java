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

package com.hazelcast.jet.impl.hazelcast;

import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.OutboundFrame;

import java.nio.ByteBuffer;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.SHORT_SIZE_IN_BYTES;

//CHECKSTYLE:OFF
public final class JetPacket extends HeapData implements OutboundFrame {
    public static final byte VERSION = 105;

    public static final int HEADER_URGENT = 4;
    public static final int HEADER_JET_DATA_CHUNK = 10;
    public static final int HEADER_JET_DATA_CHUNK_SENT = 11;
    public static final int HEADER_JET_SHUFFLER_CLOSED = 12;
    public static final int HEADER_JET_DATA_NO_APP_FAILURE = 13;
    public static final int HEADER_JET_DATA_NO_CONTAINER_FAILURE = 14;
    public static final int HEADER_JET_DATA_NO_TASK_FAILURE = 15;
    public static final int HEADER_JET_DATA_NO_MEMBER_FAILURE = 16;
    public static final int HEADER_JET_CHUNK_WRONG_CHUNK_FAILURE = 17;
    public static final int HEADER_JET_APPLICATION_IS_NOT_EXECUTING = 18;
    public static final int HEADER_JET_EXECUTION_ERROR = 19;
    public static final int HEADER_JET_MEMBER_EVENT = 20;
    // The value of these constants is important. The order needs to match the order in the read/write process
    protected static final short PERSIST_VERSION = 1;
    private static final short PERSIST_HEADER = 2;
    private static final short PERSIST_PARTITION = 3;
    private static final short PERSIST_SIZE = 4;
    private static final short PERSIST_VALUE = 5;
    private static final short PERSIST_COMPLETED = Short.MAX_VALUE;
    private static final short PERSIST_TASK_ID = 10;
    private static final short PERSIST_CONTAINER = 11;
    private static final short PERSIST_APPLICATION_SIZE = 12;
    private static final short PERSIST_APPLICATION = 13;
    protected transient Connection conn;
    protected short header;
    private int partitionId;
    // These 2 fields are only used during read/write. Otherwise they have no meaning.
    private int valueOffset;
    private int size;
    // Stores the current 'phase' of read/write. This is needed so that repeated calls can be made to read/write.
    private short persistStatus;
    private int taskID;
    private int containerId;
    private Address remoteMember;
    private byte[] applicationNameBytes;

    public JetPacket() {

    }

    public JetPacket(byte[] applicationNameBytes, byte[] payLoad) {
        this(-1, -1, applicationNameBytes, payLoad);
    }


    public JetPacket(byte[] applicationNameBytes) {
        this(-1, -1, applicationNameBytes, null);
    }

    public JetPacket(int containerId,
                     byte[] applicationNameBytes
    ) {
        this(-1, containerId, applicationNameBytes, null);
    }

    public JetPacket(int taskID,
                     int containerId,
                     byte[] applicationNameBytes
    ) {
        this(taskID, containerId, applicationNameBytes, null);
    }

    public JetPacket(int taskID,
                     int containerId,
                     byte[] applicationNameBytes,
                     byte[] payLoad
    ) {
        this(payLoad, -1);

        this.taskID = taskID;
        this.containerId = containerId;
        this.applicationNameBytes = applicationNameBytes;
    }


    public JetPacket(byte[] payload, int partitionId) {
        super(payload);
        this.partitionId = partitionId;
    }

    public boolean isHeaderSet(int bit) {
        return (header & 1 << bit) != 0;
    }

    //CHECKSTYLE:OFF
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

        if (!writeTask(destination)) {
            return false;
        }

        if (!writeContainer(destination)) {
            return false;
        }

        if (!writeApplicationNameBytesSize(destination)) {
            return false;
        }

        if (!writeApplicationNameBytes(destination)) {
            return false;
        }

        setPersistStatus(PERSIST_COMPLETED);
        return true;
    }

    private boolean writeApplicationNameBytesSize(ByteBuffer destination) {
        if (!isPersistStatusSet(PERSIST_APPLICATION_SIZE)) {
            if (destination.remaining() < this.applicationNameBytes.length) {
                return false;
            }

            destination.putInt(this.applicationNameBytes.length);
            setPersistStatus(PERSIST_APPLICATION_SIZE);
        }
        return true;
    }
    //CHECKSTYLE:ON

    //CHECKSTYLE:OFF
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

        if (!readTask(source)) {
            return false;
        }

        if (!readContainer(source)) {
            return false;
        }

        if (!readApplicationNameBytesSize(source)) {
            return false;
        }

        if (!readApplicationNameBytes(source)) {
            return false;
        }

        setPersistStatus(PERSIST_COMPLETED);

        return true;
    }

    // ========================= Task =================================================
    private boolean writeTask(ByteBuffer destination) {
        if (!isPersistStatusSet(PERSIST_TASK_ID)) {
            if (destination.remaining() <= INT_SIZE_IN_BYTES) {
                return false;
            }

            destination.putInt(this.taskID);
            setPersistStatus(PERSIST_TASK_ID);
        }
        return true;
    }
    //CHECKSTYLE:ON

    private boolean readTask(ByteBuffer source) {
        if (!isPersistStatusSet(PERSIST_TASK_ID)) {
            if (source.remaining() < INT_SIZE_IN_BYTES) {
                return false;
            }
            this.taskID = source.getInt();
            setPersistStatus(PERSIST_TASK_ID);
        }
        return true;
    }

    private boolean writeContainer(ByteBuffer destination) {
        if (!isPersistStatusSet(PERSIST_CONTAINER)) {
            if (destination.remaining() < INT_SIZE_IN_BYTES) {
                return false;
            }

            destination.putInt(this.containerId);
            setPersistStatus(PERSIST_CONTAINER);
        }

        return true;
    }

    // ========================= container =================================================

    private boolean readContainer(ByteBuffer source) {
        if (!isPersistStatusSet(PERSIST_CONTAINER)) {
            if (source.remaining() < INT_SIZE_IN_BYTES) {
                return false;
            }

            this.containerId = source.getInt();
            setPersistStatus(PERSIST_CONTAINER);
        }

        return true;
    }

    // ========================= application ==========================
    private boolean writeApplicationNameBytes(ByteBuffer destination) {
        if (!isPersistStatusSet(PERSIST_APPLICATION)) {
            if (this.applicationNameBytes.length > 0) {
                if (destination.remaining() < this.applicationNameBytes.length) {
                    return false;
                }

                destination.put(this.applicationNameBytes);
            }

            setPersistStatus(PERSIST_APPLICATION);
        }
        return true;
    }

    private boolean readApplicationNameBytesSize(ByteBuffer source) {
        if (!isPersistStatusSet(PERSIST_APPLICATION_SIZE)) {
            if (source.remaining() < INT_SIZE_IN_BYTES) {
                return false;
            }

            int size = source.getInt();
            this.applicationNameBytes = new byte[size];
            setPersistStatus(PERSIST_APPLICATION_SIZE);
        }

        return true;
    }

    private boolean readApplicationNameBytes(ByteBuffer source) {
        if (!isPersistStatusSet(PERSIST_APPLICATION)) {
            if (source.remaining() < this.applicationNameBytes.length) {
                return false;
            }

            source.get(this.applicationNameBytes);

            setPersistStatus(PERSIST_APPLICATION);
        }
        return true;
    }

    protected boolean readVersion(ByteBuffer source) {
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

    // ========================= Getters =========================

    protected boolean writeVersion(ByteBuffer destination) {
        if (!isPersistStatusSet(PERSIST_VERSION)) {
            if (!destination.hasRemaining()) {
                return false;
            }
            destination.put(VERSION);
            setPersistStatus(PERSIST_VERSION);
        }
        return true;
    }

    public int getTaskID() {
        return this.taskID;
    }

    public int getContainerId() {
        return this.containerId;
    }

    public byte[] getApplicationNameBytes() {
        return this.applicationNameBytes;
    }

    @Override
    public boolean isUrgent() {
        return isHeaderSet(HEADER_URGENT);
    }

    protected void setPersistStatus(short persistStatus) {
        this.persistStatus = persistStatus;
    }

    protected boolean isPersistStatusSet(short status) {
        return this.persistStatus >= status;
    }

    protected boolean readHeader(ByteBuffer src) {
        if (!isPersistStatusSet(PERSIST_HEADER)) {
            if (src.remaining() < 2) {
                return false;
            }
            header = src.getShort();
            setPersistStatus(PERSIST_HEADER);
        }
        return true;
    }

    // ========================= header =================================================

    protected boolean writeHeader(ByteBuffer dst) {
        if (!isPersistStatusSet(PERSIST_HEADER)) {
            if (dst.remaining() < SHORT_SIZE_IN_BYTES) {
                return false;
            }
            dst.putShort(header);
            setPersistStatus(PERSIST_HEADER);
        }
        return true;
    }

    protected boolean readPartition(ByteBuffer src) {
        if (!isPersistStatusSet(PERSIST_PARTITION)) {
            if (src.remaining() < INT_SIZE_IN_BYTES) {
                return false;
            }
            partitionId = src.getInt();
            setPersistStatus(PERSIST_PARTITION);
        }
        return true;
    }

    // ========================= partition =================================================

    protected boolean writePartition(ByteBuffer dst) {
        if (!isPersistStatusSet(PERSIST_PARTITION)) {
            if (dst.remaining() < INT_SIZE_IN_BYTES) {
                return false;
            }
            dst.putInt(partitionId);
            setPersistStatus(PERSIST_PARTITION);
        }
        return true;
    }

    protected boolean readSize(ByteBuffer src) {
        if (!isPersistStatusSet(PERSIST_SIZE)) {
            if (src.remaining() < INT_SIZE_IN_BYTES) {
                return false;
            }
            size = src.getInt();
            setPersistStatus(PERSIST_SIZE);
        }
        return true;
    }


    // ========================= size =================================================

    protected boolean writeSize(ByteBuffer dst) {
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

    protected boolean readValue(ByteBuffer src) {
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

    // ========================= value =================================================

    protected boolean writeValue(ByteBuffer dst) {
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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JetPacket{").append("header=").
                append(header).
                append(", containerId=").append(this.containerId).
                append(", applicationName=").
                append(
                        this.applicationNameBytes == null
                                ?
                                "null"
                                :
                                new String(this.applicationNameBytes)
                ).
                append(", taskID=").append(this.taskID).
                append(", conn=").append(this.conn == null ? "null" : this.conn).
                append('}');

        return sb.toString();
    }

    public short getHeader() {
        return this.header;
    }

    public void setHeader(int bit) {
        this.header = (short) bit;
    }

    public void reset() {
        payload = null;
        persistStatus = 0;
    }

    public Address getRemoteMember() {
        return this.remoteMember;
    }

    public void setRemoteMember(Address remoteMember) {
        this.remoteMember = remoteMember;
    }
}
//CHECKSTYLE:ON
