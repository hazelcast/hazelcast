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
import com.hazelcast.nio.serialization.DataWriter;
import com.hazelcast.nio.serialization.SerializationContext;

import java.nio.ByteBuffer;

public final class Packet extends DataWriter implements SocketWritable {

    public static final byte PACKET_VERSION = 1;

    public static final int HEADER_OP = 0;
    public static final int HEADER_EVENT = 1;
    public static final int HEADER_MIGRATION = 2;

    private byte header;

    private transient Connection conn;

    public Packet(SerializationContext context) {
        super(context);
    }

    public Packet(Data value, SerializationContext context) {
        super(value, context);
    }

    public Connection getConn() {
        return conn;
    }

    public void setConn(final Connection conn) {
        this.conn = conn;
    }

    public void setHeader(int bit, boolean b) {
        if (b)
            header |= 1 << bit;
        else
            header &= ~1 << bit;
    }

    public boolean isHeaderSet(int bit) {
        return (header & 1 << bit) != 0;
    }

    public byte getHeader() {
        return header;
    }

    public void onEnqueue() {
    }

    public final boolean writeTo(ByteBuffer destination) {
        // TODO: @mm - think about packet versions
        if (!isStatusSet(stHeader)) {
            if (!destination.hasRemaining()) {
                return false;
            }
            destination.put(header);
            setStatus(stHeader);
        }
        return super.writeTo(destination);
    }

    public final boolean readFrom(ByteBuffer source) {
        // TODO: @mm - think about packet versions
        if (!isStatusSet(stHeader)) {
            if (!source.hasRemaining()) {
                return false;
            }
            header = source.get();
            setStatus(stHeader);
        }
        return super.readFrom(source);
    }

    public int size() {
        return (data == null) ? 1 : data.totalSize() + 1;
    }
}
