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

import java.nio.ByteBuffer;

/**
 * @mdogan 1/23/13
 */
public class DataWriter {

    protected static final int stHeader = 0;
    protected static final int stType = 1;
    protected static final int stClassId = 2;
    protected static final int stVersion = 3;
    protected static final int stClassDefSize = 4;
    protected static final int stClassDef = 5;
    protected static final int stSize = 6;
    protected static final int stValue = 7;
    protected static final int stHash = 8;
    protected static final int stAll = 9;

    private ByteBuffer buffer;
    private int classId = 0;
    private int version = 0;
    private int classDefSize = 0;
    protected Data data;

    private transient short status = 0;
    private transient SerializationContext context;

    public DataWriter(Data data) {
        this.data = data;
    }

    public DataWriter(SerializationContext context) {
        this.context = context;
    }

    public DataWriter(Data data, SerializationContext context) {
        this.data = data;
        this.context = context;
    }

    /**
     * WARNING:
     *
     * Should be in sync with {@link Data#writeData(ObjectDataOutput)}
     */
    public boolean writeTo(ByteBuffer destination) {
        if (!isStatusSet(stType)) {
            if (destination.remaining() < 4) {
                return false;
            }
            destination.putInt(data.type);
            setStatus(stType);
        }
        if (!isStatusSet(stClassId)) {
            if (destination.remaining() < 4) {
                return false;
            }
            final int classId = data.cd == null ? Data.NO_CLASS_ID : data.cd.getClassId();
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
            final int version = data.cd.getVersion();
            destination.putInt(version);
            setStatus(stVersion);
        }
        if (!isStatusSet(stClassDefSize)) {
            if (destination.remaining() < 4) {
                return false;
            }
            final byte[] binary = data.cd.getBinary();
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
            final int size = data.bufferSize();
            destination.putInt(size);
            setStatus(stSize);
            if (size <= 0) {
                setStatus(stValue);
            } else {
                buffer = ByteBuffer.wrap(data.buffer);
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
            destination.putInt(data.getPartitionHash());
            setStatus(stHash);
        }
        setStatus(stAll);
        return true;
    }

    /**
     * WARNING:
     *
     * Should be in sync with {@link Data#readData(ObjectDataInput)}
     */
    public boolean readFrom(ByteBuffer source) {
        if (data == null) {
            data = new Data();
        }
        if (!isStatusSet(stType)) {
            if (source.remaining() < 4) {
                return false;
            }
            data.type = source.getInt();
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
                data.cd = cd;
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
                    data.cd = new ClassDefinitionBinaryProxy(classId, version, binary);
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
            data.buffer = buffer.array();
            setStatus(stValue);
        }
        if (!isStatusSet(stHash)) {
            if (source.remaining() < 4) {
                return false;
            }
            data.setPartitionHash(source.getInt());
            setStatus(stHash);
        }
        setStatus(stAll);
        return true;
    }

    protected final void setStatus(int bit) {
        status |= 1 << bit;
    }

    protected final boolean isStatusSet(int bit) {
        return (status & 1 << bit) != 0;
    }

    public final Data getData() {
        data.postConstruct(context);
        return data;
    }

    public final void setData(Data data) {
        this.data = data;
    }

    public boolean done() {
        return isStatusSet(stAll);
    }

    public void reset() {
        buffer = null;
        classId = 0;
        version = 0;
        classDefSize = 0;
        data = null;
        status = 0;
    }
}
