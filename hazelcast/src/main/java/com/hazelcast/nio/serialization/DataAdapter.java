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

package com.hazelcast.nio.serialization;

import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.SocketReadable;
import com.hazelcast.nio.SocketWritable;

import java.nio.ByteBuffer;

/**
 * @mdogan 1/23/13
 */
public class DataAdapter implements SocketWritable, SocketReadable {

    protected static int stBit = 0;

    private static final int stType = stBit++;
    private static final int stClassId = stBit++;
    private static final int stFactoryId = stBit++;
    private static final int stVersion = stBit++;
    private static final int stClassDefSize = stBit++;
    private static final int stClassDef = stBit++;
    private static final int stSize = stBit++;
    private static final int stValue = stBit++;
    private static final int stHash = stBit++;
    private static final int stAll = stBit++;

    private ByteBuffer buffer;
    private int factoryId = 0;
    private int classId = 0;
    private int version = 0;
    private int classDefSize = 0;
    private boolean skipClassDef = false;
    protected Data data;

    private transient short status = 0;
    private transient SerializationContext context;

    public DataAdapter(Data data) {
        this.data = data;
    }

    public DataAdapter(SerializationContext context) {
        this.context = context;
    }

    public DataAdapter(Data data, SerializationContext context) {
        this.data = data;
        this.context = context;
    }

    /**
     * WARNING:
     *
     * Should be in sync with {@link Data#writeData(com.hazelcast.nio.ObjectDataOutput)}
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
            classId = data.classDefinition == null ? Data.NO_CLASS_ID : data.classDefinition.getClassId();
            destination.putInt(classId);
            if (classId == Data.NO_CLASS_ID) {
                setStatus(stFactoryId);
                setStatus(stVersion);
                setStatus(stClassDefSize);
                setStatus(stClassDef);
            }
            setStatus(stClassId);
        }
        if (!isStatusSet(stFactoryId)) {
            if (destination.remaining() < 4) {
                return false;
            }
            destination.putInt(data.classDefinition.getFactoryId());
            setStatus(stFactoryId);
        }
        if (!isStatusSet(stVersion)) {
            if (destination.remaining() < 4) {
                return false;
            }
            final int version = data.classDefinition.getVersion();
            destination.putInt(version);
            setStatus(stVersion);
        }
        if (!isStatusSet(stClassDefSize)) {
            if (destination.remaining() < 4) {
                return false;
            }
            final BinaryClassDefinition cd = (BinaryClassDefinition) data.classDefinition;
            final byte[] binary = cd.getBinary();
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
     * Should be in sync with {@link Data#readData(com.hazelcast.nio.ObjectDataInput)}
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
                setStatus(stFactoryId);
                setStatus(stVersion);
                setStatus(stClassDefSize);
                setStatus(stClassDef);
            }
        }
        if (!isStatusSet(stFactoryId)) {
            if (source.remaining() < 4) {
                return false;
            }
            factoryId = source.getInt();
            setStatus(stFactoryId);
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
            if (!skipClassDef && (cd = context.lookup(factoryId, classId, version)) != null) {
                data.classDefinition = cd;
                skipClassDef = true;
            }
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
                if (skipClassDef) {
                    source.position(classDefSize + source.position());
                } else {
                    final byte[] binary = new byte[classDefSize];
                    source.get(binary);
                    data.classDefinition = new BinaryClassDefinitionProxy(factoryId, classId, version, binary);
                }
                setStatus(stClassDef);
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

    public void onEnqueue() {
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
