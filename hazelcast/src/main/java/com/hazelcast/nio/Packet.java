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

import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBinaryProxy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationContext;

import java.nio.ByteBuffer;

public class Packet implements SocketWritable {

    public static final byte PACKET_VERSION = 1;

    public static final int HEADER_OP = 0;
    public static final int HEADER_EVENT = 1;

    private transient boolean stHeader;
    private transient boolean stType;
    private transient boolean stClassId;
    private transient boolean stVersion;
    private transient boolean stClassDefSize;
    private transient boolean stClassDef;
    private transient boolean stSize;
    private transient boolean stValue;
    private transient boolean stHash;

    private byte header;
    private ByteBuffer buffer;
    private int classId = 0;
    private int version = 0;
    private int classDefSize = 0;
    private Data value;

    private transient Connection conn;
    private transient SerializationContext context;

    public Packet(SerializationContext context) {
        this.context = context;
    }

    public Packet(Data value, SerializationContext context) {
        this(value, null, context);
    }

    public Packet(Data value, Connection conn, SerializationContext context) {
        this.value = value;
        this.conn = conn;
        this.context = context;
    }

    public Data getValue() {
        value.postConstruct(context);
        return value;
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
            header &= ~1 << bit;
    }

    public boolean isHeaderSet(int bit) {
        return (header & 1 << bit) != 0;
    }

    public void onEnqueue() {

    }

    public final boolean writeTo(ByteBuffer destination) {
        // TODO: think about packet versions
        if (!stHeader) {
            if (!destination.hasRemaining()) {
                return false;
            }
            destination.put(header);
            stHeader = true;
        }
        if (!stType) {
            if (destination.remaining() < 4) {
                return false;
            }
            destination.putInt(value.type);
            stType = true;
        }
        if (!stClassId) {
            if (destination.remaining() < 4) {
                return false;
            }
            final int classId = value.cd == null ? Data.NO_CLASS_ID : value.cd.getClassId();
            destination.putInt(classId);
            if (classId == Data.NO_CLASS_ID) {
                stVersion = true;
                stClassDefSize = true;
                stClassDef = true;
            }
            stClassId = true;
        }
        if (!stVersion) {
            if (destination.remaining() < 4) {
                return false;
            }
            final int version = value.cd.getVersion();
            destination.putInt(version);
            stVersion = true;
        }
        if (!stClassDefSize) {
            if (destination.remaining() < 4) {
                return false;
            }
            final byte[] binary = value.cd.getBinary();
            classDefSize = binary == null ? 0 : binary.length;
            destination.putInt(classDefSize);
            stClassDefSize = true;
            if (classDefSize == 0) {
                stClassDef = true;
            } else {
                buffer = ByteBuffer.wrap(binary);
            }
        }
        if (!stClassDef) {
            IOUtil.copyToHeapBuffer(buffer, destination);
            if (buffer.hasRemaining()) {
                return false;
            }
            stClassDef = true;
        }
        if (!stSize) {
            if (destination.remaining() < 4) {
                return false;
            }
            final int size = value.size();
            destination.putInt(size);
            stSize = true;
            if (size <= 0) {
                stValue = true;
            } else {
                buffer = ByteBuffer.wrap(value.buffer);
            }
        }
        if (!stValue) {
            IOUtil.copyToHeapBuffer(buffer, destination);
            if (buffer.hasRemaining()) {
                return false;
            }
            stValue = true;
        }
        if (!stHash) {
            if (destination.remaining() < 4) {
                return false;
            }
            destination.putInt(value.getPartitionHash());
            stHash = true;
        }
        return true;
    }

    public final boolean readFrom(ByteBuffer source) {
        // TODO: think about packet versions
        if (!stHeader) {
            if (!source.hasRemaining()) {
                return false;
            }
            header = source.get();
            stHeader = true;
        }
        if (value == null) {
            value = new Data();
        }
        if (!stType) {
            if (source.remaining() < 4) {
                return false;
            }
            value.type = source.getInt();
            stType = true;
        }
        if (!stClassId) {
            if (source.remaining() < 4) {
                return false;
            }
            classId = source.getInt();
            stClassId = true;
            if (classId == Data.NO_CLASS_ID) {
                stVersion = true;
                stClassDefSize = true;
                stClassDef = true;
            }
        }
        if (!stVersion) {
            if (source.remaining() < 4) {
                return false;
            }
            version = source.getInt();
            stVersion = true;
        }
        if (!stClassDef) {
            ClassDefinition cd;
            if ((cd = context.lookup(classId, version)) != null) {
                value.cd = cd;
                stClassDefSize = true;
                stClassDef = true;
            } else {
                if (!stClassDefSize) {
                    if (source.remaining() < 4) {
                        return false;
                    }
                    classDefSize = source.getInt();
                    stClassDefSize = true;
                }
                if (!stClassDef) {
                    if (source.remaining() < classDefSize) {
                        return false;
                    }
                    final byte[] binary = new byte[classDefSize];
                    source.get(binary);
                    value.cd = new ClassDefinitionBinaryProxy(classId, version, binary);
                    stClassDef = true;
                }
            }
        }
        if (!stSize) {
            if (source.remaining() < 4) {
                return false;
            }
            final int size = source.getInt();
            buffer = ByteBuffer.allocate(size);
            stSize = true;
        }
        if (!stValue) {
            IOUtil.copyToHeapBuffer(source, buffer);
            if (buffer.hasRemaining()) {
                return false;
            }
            buffer.flip();
            value.buffer = buffer.array();
            stValue = true;
        }
        if (!stHash) {
            if (source.remaining() < 4) {
                return false;
            }
            value.setPartitionHash(source.getInt());
            stHash = true;
        }
        return true;
    }
}
