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
import com.hazelcast.nio.serialization.DataAdapter;
import com.hazelcast.nio.serialization.SerializationContext;

import java.nio.ByteBuffer;

public final class Packet extends DataAdapter implements SocketWritable, SocketReadable {

    public static final byte PACKET_VERSION = 1;

    private static final int stHeader = stBit++;
    private static final int stPartition = stBit++;

    public static final int HEADER_OP = 0;
    public static final int HEADER_RESPONSE = 1;
    public static final int HEADER_EVENT = 2;
    public static final int HEADER_WAN_REPLICATION = 3;

    private short header;
    private int partitionId;

    private transient Connection conn;

    public Packet(SerializationContext context) {
        super(context);
    }

    public Packet(Data value, SerializationContext context) {
        this(value, -1, context);
    }

    public Packet(Data value, int partitionId, SerializationContext context) {
        super(value, context);
        this.partitionId = partitionId;
    }

    public Connection getConn() {
        return conn;
    }

    public void setConn(final Connection conn) {
        this.conn = conn;
    }

    public void setHeader(int bit) {
        header |= 1 << bit;
    }

    public boolean isHeaderSet(int bit) {
        return (header & 1 << bit) != 0;
    }

    public short getHeader() {
        return header;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public final boolean writeTo(ByteBuffer destination) {
        // TODO: @mm - think about packet versions
        if (!isStatusSet(stHeader)) {
            if (destination.remaining() < 2) {
                return false;
            }
            destination.putShort(header);
            setStatus(stHeader);
        }
        if (!isStatusSet(stPartition)) {
            if (destination.remaining() < 4) {
                return false;
            }
            destination.putInt(partitionId);
            setStatus(stPartition);
        }
        return super.writeTo(destination);
    }

    public final boolean readFrom(ByteBuffer source) {
        // TODO: @mm - think about packet versions
        if (!isStatusSet(stHeader)) {
            if (source.remaining() < 2) {
                return false;
            }
            header = source.getShort();
            setStatus(stHeader);
        }
        if (!isStatusSet(stPartition)) {
            if (source.remaining() < 4) {
                return false;
            }
            partitionId = source.getInt();
            setStatus(stPartition);
        }
        return super.readFrom(source);
    }

    public int size() {
        return (data != null  ? data.totalSize() : 0) + 6; // 6 = short(header) + int(partitionId)
    }
}
