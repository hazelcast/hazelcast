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

    public static final int HEADER_OP = 0;
    public static final int HEADER_EVENT = 1;
//    public static final int HEADER_RESERVED_2 = 2;
//    public static final int HEADER_RESERVED_3 = 3;
//    public static final int HEADER_RESERVED_4 = 4;
//    public static final int HEADER_RESERVED_5 = 5;
//    public static final int HEADER_RESERVED_6 = 6;

    private static final byte ST_HEADER = 0x1;
    private static final byte ST_SIZE = 0x2;
    private static final byte ST_VALUE = 0x4;

    private byte header;
    private DataHolder value;

    private transient byte status = 0x0;
    private transient Connection conn;
    private transient CallState callState = null;

    public Packet() {
    }

    public Packet(Data value) {
        this(value, null);
    }

    public Packet(Data value, Connection conn) {
        this.value = value == null || value.size() == 0 ? null : new DataHolder(value);
        this.conn = conn;
    }

    public Data getValue() {
        return value.toData();
    }

    public Connection getConn() {
        return conn;
    }

    void setConn(final Connection conn) {
        this.conn = conn;
    }

    public void setHeader(int bit, boolean b) {
        if (b)
            header |= 1 << bit;
        else
            header &= ~ 1 << bit;
    }

    public boolean isHeaderSet(int bit) {
        return (header & 1 << bit) != 0;
    }

    public void onEnqueue() {

    }

    public final boolean writeTo(ByteBuffer destination) {
        // TODO: think about packet versions
        if (!isStatusSet(ST_HEADER)) {
            if (!destination.hasRemaining()) {
                return false;
            }
            destination.put(header);
            setStatus(ST_HEADER);
        }

        if (!isStatusSet(ST_SIZE)) {
            if (destination.remaining() < 4) {
                return false;
            }
            destination.putInt(value != null ? value.size() : 0);
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
        if (!isStatusSet(ST_HEADER)) {
            if (!source.hasRemaining()) {
                return false;
            }
            header = source.get();
            setStatus(ST_HEADER);
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

    public CallState getCallState() {
        return callState;
    }
}
