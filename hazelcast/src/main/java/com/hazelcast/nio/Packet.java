/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.logging.CallState;
import com.hazelcast.logging.CallStateAware;

import java.nio.ByteBuffer;

public class Packet implements SocketWritable, CallStateAware {

    public static final byte PACKET_VERSION = 1;

    private static final byte ST_PARTITION  = 0x1;
    private static final byte ST_SIZE       = 0x2;
    private static final byte ST_VALUE      = 0x4;

    private DataHolder value;
    private int partitionId;

    private transient byte status = 0x0;
    private transient Connection conn;
    private transient CallState callState = null;

    public Packet() {
    }

    public Packet(Data value, int partitionId) {
        this(value, partitionId, null);
    }

    public Packet(Data value, int partitionId, Connection conn) {
        this.value = value == null || value.size() == 0 ? null : new DataHolder(value);
        this.partitionId = partitionId;
        this.conn = conn;
    }

    public Data getValue() {
        return value.toData();
    }

    public int getPartitionId() {
        return partitionId;
    }

    public Connection getConn() {
        return conn;
    }

    void setConn(final Connection conn) {
        this.conn = conn;
    }

    public void onEnqueue() {

    }

    public final boolean writeTo(ByteBuffer destination) {
        // TODO: think about packet versions
        if (!isStatusSet(ST_PARTITION)) {
            if (!writeInt(destination, partitionId)) {
                return false;
            }
            setStatus(ST_PARTITION);
        }

        if (!isStatusSet(ST_SIZE)) {
            if (!writeInt(destination, value != null ? value.size() : 0)) {
                return false;
            }
            setStatus(ST_SIZE);
        }

        if (isStatusSet(ST_SIZE) && !isStatusSet(ST_VALUE)) {
            if (value != null) {
                IOUtil.copyToHeapBuffer(value.buffer, destination);
                if (value.buffer.hasRemaining()) {
                    return false;
                }
            }
            setStatus(ST_VALUE);
        }
        return true;
    }

    public final boolean readFrom(ByteBuffer source) {
        // TODO: think about packet versions
        if (!isStatusSet(ST_PARTITION)) {
            if (source.remaining() < 4) {
                return false;
            }
            partitionId = source.getInt();
            setStatus(ST_PARTITION);
        }

        if (!isStatusSet(ST_SIZE)) {
            if (source.remaining() < 4) {
                return false;
            }
            final int size = source.getInt();
            if (value == null || value.size() < size) {
                value = new DataHolder(size);
            }
            setStatus(ST_SIZE);
        }

        if (isStatusSet(ST_SIZE) && !isStatusSet(ST_VALUE)) {
            IOUtil.copyToHeapBuffer(source, value.buffer);
            if (value.shouldRead()) {
                return false;
            }
            value.postRead();
            setStatus(ST_VALUE);
        }
        return true;
    }

    private void setStatus(byte st) {
        status |= st;
    }

    private boolean isStatusSet(byte st) {
        return (status & st) != 0;
    }

    private static boolean writeInt(ByteBuffer destination, int value) {
        if (destination.remaining() >= 4) {
            destination.putInt(value);
            return true;
        }
        return false;
    }

    public CallState getCallState() {
        return callState;
    }
}
