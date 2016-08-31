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

package com.hazelcast.jet.impl.data.io;

import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.OutboundFrame;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.SHORT_SIZE_IN_BYTES;

@SuppressWarnings({
        "checkstyle:npathcomplexity",
        "checkstyle:cyclomaticcomplexity",
        "checkstyle:methodcount"
}
)
public final class JetPacket extends HeapData implements OutboundFrame {
    public static final byte VERSION = 105;

    public static final int HEADER_URGENT = 4;
    public static final int HEADER_JET_DATA_CHUNK = 10;
    public static final int HEADER_JET_DATA_CHUNK_SENT = 11;
    public static final int HEADER_JET_SHUFFLER_CLOSED = 12;
    public static final int HEADER_JET_DATA_NO_APP_FAILURE = 13;
    public static final int HEADER_JET_DATA_NO_VERTEX_RUNNER_FAILURE = 14;
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
    private static final short PERSIST_VERTEX_RUNNER = 11;
    private static final short PERSIST_APPLICATION_SIZE = 12;
    private static final short PERSIST_APPLICATION = 13;
    protected short header;
    private int partitionId;
    // These 2 fields are only used during read/write. Otherwise they have no meaning.
    private int valueOffset;
    private int size;
    // Stores the current 'phase' of read/write. This is needed so that repeated calls can be made to read/write.
    private short persistStatus;
    private int taskId;
    private int vertexRunnerId;
    private Address remoteMember;
    private byte[] jobNameBytes;

    public JetPacket() {

    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public JetPacket(byte[] jobNameBytes, byte[] payload) {
        this(-1, -1, jobNameBytes, payload);
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public JetPacket(byte[] jobNameBytes) {
        this(-1, -1, jobNameBytes, null);
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public JetPacket(int vertexRunnerId,
                     byte[] jobNameBytes
    ) {
        this(-1, vertexRunnerId, jobNameBytes, null);
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public JetPacket(int taskId,
                     int vertexRunnerId,
                     byte[] jobNameBytes
    ) {
        this(taskId, vertexRunnerId, jobNameBytes, null);
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public JetPacket(int taskId,
                     int vertexRunnerId,
                     byte[] jobNameBytes,
                     byte[] payload
    ) {
        this(payload, -1);

        this.taskId = taskId;
        this.vertexRunnerId = vertexRunnerId;
        this.jobNameBytes = jobNameBytes;
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
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

        if (!writeVertexRunner(destination)) {
            return false;
        }

        if (!writeApplicationNameBytesSize(destination)) {
            return false;
        }

        if (!writeJobNameBytes(destination)) {
            return false;
        }

        setPersistStatus(PERSIST_COMPLETED);
        return true;
    }

    private boolean writeApplicationNameBytesSize(ByteBuffer destination) {
        if (!isPersistStatusSet(PERSIST_APPLICATION_SIZE)) {
            if (destination.remaining() < this.jobNameBytes.length) {
                return false;
            }

            destination.putInt(this.jobNameBytes.length);
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

        if (!readVertexRunner(source)) {
            return false;
        }

        if (!readJobNameBytesSize(source)) {
            return false;
        }

        if (!readJobNameBytes(source)) {
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

            destination.putInt(this.taskId);
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
            this.taskId = source.getInt();
            setPersistStatus(PERSIST_TASK_ID);
        }
        return true;
    }

    private boolean writeVertexRunner(ByteBuffer destination) {
        if (!isPersistStatusSet(PERSIST_VERTEX_RUNNER)) {
            if (destination.remaining() < INT_SIZE_IN_BYTES) {
                return false;
            }

            destination.putInt(this.vertexRunnerId);
            setPersistStatus(PERSIST_VERTEX_RUNNER);
        }

        return true;
    }

    // ========================= vertex runner =================================================

    private boolean readVertexRunner(ByteBuffer source) {
        if (!isPersistStatusSet(PERSIST_VERTEX_RUNNER)) {
            if (source.remaining() < INT_SIZE_IN_BYTES) {
                return false;
            }

            this.vertexRunnerId = source.getInt();
            setPersistStatus(PERSIST_VERTEX_RUNNER);
        }

        return true;
    }

    // ========================= job ==========================
    private boolean writeJobNameBytes(ByteBuffer destination) {
        if (!isPersistStatusSet(PERSIST_APPLICATION)) {
            if (this.jobNameBytes.length > 0) {
                if (destination.remaining() < this.jobNameBytes.length) {
                    return false;
                }

                destination.put(this.jobNameBytes);
            }

            setPersistStatus(PERSIST_APPLICATION);
        }
        return true;
    }

    private boolean readJobNameBytesSize(ByteBuffer source) {
        if (!isPersistStatusSet(PERSIST_APPLICATION_SIZE)) {
            if (source.remaining() < INT_SIZE_IN_BYTES) {
                return false;
            }

            int size = source.getInt();
            this.jobNameBytes = new byte[size];
            setPersistStatus(PERSIST_APPLICATION_SIZE);
        }

        return true;
    }

    private boolean readJobNameBytes(ByteBuffer source) {
        if (!isPersistStatusSet(PERSIST_APPLICATION)) {
            if (source.remaining() < this.jobNameBytes.length) {
                return false;
            }

            source.get(this.jobNameBytes);

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

    public int getTaskId() {
        return this.taskId;
    }

    public int getVertexRunnerId() {
        return this.vertexRunnerId;
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public byte[] getJobNameBytes() {
        return this.jobNameBytes;
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
                append(", vertexRunnerId=").append(this.vertexRunnerId).
                append(", taskId=").append(this.taskId).
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        if (!super.equals(o)) {
            return false;
        }

        JetPacket jetPacket = (JetPacket) o;

        if (header != jetPacket.header) {
            return false;
        }
        if (partitionId != jetPacket.partitionId) {
            return false;
        }
        if (valueOffset != jetPacket.valueOffset) {
            return false;
        }
        if (size != jetPacket.size) {
            return false;
        }
        if (persistStatus != jetPacket.persistStatus) {
            return false;
        }
        if (taskId != jetPacket.taskId) {
            return false;
        }
        if (vertexRunnerId != jetPacket.vertexRunnerId) {
            return false;
        }
        if (remoteMember != null ? !remoteMember.equals(jetPacket.remoteMember) : jetPacket.remoteMember != null) {
            return false;
        }

        return Arrays.equals(jobNameBytes, jetPacket.jobNameBytes);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (int) header;
        result = 31 * result + partitionId;
        result = 31 * result + valueOffset;
        result = 31 * result + size;
        result = 31 * result + (int) persistStatus;
        result = 31 * result + taskId;
        result = 31 * result + vertexRunnerId;
        result = 31 * result + (remoteMember != null ? remoteMember.hashCode() : 0);
        result = 31 * result + (jobNameBytes != null ? Arrays.hashCode(jobNameBytes) : 0);
        return result;
    }
}
