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

import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBinaryProxy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationContext;
import com.hazelcast.spi.Connection;

import java.nio.ByteBuffer;

public class Packet implements SocketWritable {

    public static final byte PACKET_VERSION = 1;

    public static final int HEADER_OP = 0;
    public static final int HEADER_EVENT = 1;

    private static final int stHeader = 0;
    private static final int stType = 1 ;
    private static final int stClassId = 2;
    private static final int stVersion = 3;
    private static final int stClassDefSize = 4;
    private static final int stClassDef = 5;
    private static final int stSize = 6;
    private static final int stValue = 7;
    private static final int stHash = 8;

    private byte header;
    private ByteBuffer buffer;
    private int classId = 0;
    private int version = 0;
    private int classDefSize = 0;
    private Data value;

    private transient short status = 0;
    private transient Connection conn;
    private transient SerializationContext context;

    public Packet(SerializationContext context) {
        this.context = context;
    }

    public Packet(Data value, SerializationContext context) {
        this.value = value;
        this.context = context;
    }

    public Data getValue() {
        value.postConstruct(context);
        return value;
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

    public void onEnqueue() {

    }

    public final boolean writeTo(ByteBuffer destination) {
        // TODO: think about packet versions
        if (!isStatusSet(stHeader)) {
            if (!destination.hasRemaining()) {
                return false;
            }
            destination.put(header);
            setStatus(stHeader);
        }
        if (!isStatusSet(stType)) {
            if (destination.remaining() < 4) {
                return false;
            }
            destination.putInt(value.type);
            setStatus(stType);
        }
        if (!isStatusSet(stClassId)) {
            if (destination.remaining() < 4) {
                return false;
            }
            final int classId = value.cd == null ? Data.NO_CLASS_ID : value.cd.getClassId();
            destination.putInt(classId);
            if (classId == Data.NO_CLASS_ID) {
                setStatus(stVersion);
                setStatus(stClassDefSize);
                setStatus(stClassDef);
            }
            setStatus(stClassId);
        }
        if (!isStatusSet(stVersion)) {
            if (destination.remaining() < 4) {
                return false;
            }
            final int version = value.cd.getVersion();
            destination.putInt(version);
            setStatus(stVersion);
        }
        if (!isStatusSet(stClassDefSize)) {
            if (destination.remaining() < 4) {
                return false;
            }
            final byte[] binary = value.cd.getBinary();
            classDefSize = binary == null ? 0 : binary.length;
            destination.putInt(classDefSize);
            setStatus(stClassDefSize);
            if (classDefSize == 0) {
                setStatus(stClassDef);
            } else {
                buffer = ByteBuffer.wrap(binary);
            }
        }
        if (!isStatusSet(stClassDef)) {
            IOUtil.copyToHeapBuffer(buffer, destination);
            if (buffer.hasRemaining()) {
                return false;
            }
            setStatus(stClassDef);
        }
        if (!isStatusSet(stSize)) {
            if (destination.remaining() < 4) {
                return false;
            }
            final int size = value.bufferSize();
            destination.putInt(size);
            setStatus(stSize);
            if (size <= 0) {
                setStatus(stValue);
            } else {
                buffer = ByteBuffer.wrap(value.buffer);
            }
        }
        if (!isStatusSet(stValue)) {
            IOUtil.copyToHeapBuffer(buffer, destination);
            if (buffer.hasRemaining()) {
                return false;
            }
            setStatus(stValue);
        }
        if (!isStatusSet(stHash)) {
            if (destination.remaining() < 4) {
                return false;
            }
            destination.putInt(value.getPartitionHash());
            setStatus(stHash);
        }
        return true;
    }

    public final boolean readFrom(ByteBuffer source) {
        // TODO: think about packet versions
        if (!isStatusSet(stHeader)) {
            if (!source.hasRemaining()) {
                return false;
            }
            header = source.get();
            setStatus(stHeader);
        }
        if (value == null) {
            value = new Data();
        }
        if (!isStatusSet(stType)) {
            if (source.remaining() < 4) {
                return false;
            }
            value.type = source.getInt();
            setStatus(stType);
        }
        if (!isStatusSet(stClassId)) {
            if (source.remaining() < 4) {
                return false;
            }
            classId = source.getInt();
            setStatus(stClassId);
            if (classId == Data.NO_CLASS_ID) {
                setStatus(stVersion);
                setStatus(stClassDefSize);
                setStatus(stClassDef);
            }
        }
        if (!isStatusSet(stVersion)) {
            if (source.remaining() < 4) {
                return false;
            }
            version = source.getInt();
            setStatus(stVersion);
        }
        if (!isStatusSet(stClassDef)) {
            ClassDefinition cd;
            if ((cd = context.lookup(classId, version)) != null) {
                value.cd = cd;
                setStatus(stClassDefSize);
                setStatus(stClassDef);
            } else {
                if (!isStatusSet(stClassDefSize)) {
                    if (source.remaining() < 4) {
                        return false;
                    }
                    classDefSize = source.getInt();
                    setStatus(stClassDefSize);
                }
                if (!isStatusSet(stClassDef)) {
                    if (source.remaining() < classDefSize) {
                        return false;
                    }
                    final byte[] binary = new byte[classDefSize];
                    source.get(binary);
                    value.cd = new ClassDefinitionBinaryProxy(classId, version, binary);
                    setStatus(stClassDef);
                }
            }
        }
        if (!isStatusSet(stSize)) {
            if (source.remaining() < 4) {
                return false;
            }
            final int size = source.getInt();
            buffer = ByteBuffer.allocate(size);
            setStatus(stSize);
        }
        if (!isStatusSet(stValue)) {
            IOUtil.copyToHeapBuffer(source, buffer);
            if (buffer.hasRemaining()) {
                return false;
            }
            buffer.flip();
            value.buffer = buffer.array();
            setStatus(stValue);
        }
        if (!isStatusSet(stHash)) {
            if (source.remaining() < 4) {
                return false;
            }
            value.setPartitionHash(source.getInt());
            setStatus(stHash);
        }
        return true;
    }

    private void setStatus(int bit) {
        status |= 1 << bit;
    }

    private boolean isStatusSet(int bit) {
        return (status & 1 << bit) != 0;
    }

    public void reset() {
        status = 0;
        header = 0;
        buffer = null;
        classId = 0;
        version = 0;
        classDefSize = 0;
        value = null;
    }
}
