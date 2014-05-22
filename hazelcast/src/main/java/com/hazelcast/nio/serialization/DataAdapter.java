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

public class DataAdapter implements SocketWritable, SocketReadable {

    private static final int ST_TYPE = 1;
    private static final int ST_CLASS_ID = 2;
    private static final int ST_FACTORY_ID = 3;
    private static final int ST_VERSION = 4;
    private static final int ST_CLASS_DEF_SIZE = 5;
    private static final int ST_CLASS_DEF = 6;
    private static final int ST_SIZE = 7;
    private static final int ST_VALUE = 8;
    private static final int ST_HASH = 9;
    private static final int ST_ALL = 10;

    protected Data data;

    private ByteBuffer buffer;
    private int factoryId;
    private int classId;
    private int version;
    private int classDefSize;
    private boolean skipClassDef;

    private transient short status;
    private transient PortableContext context;

    public DataAdapter(Data data) {
        this.data = data;
    }

    public DataAdapter(PortableContext context) {
        this.context = context;
    }

    public DataAdapter(Data data, PortableContext context) {
        this.data = data;
        this.context = context;
    }

    @Override
    public boolean isUrgent() {
        return false;
    }

    /**
     * WARNING:
     * <p/>
     * Should be in sync with {@link Data#writeData(com.hazelcast.nio.ObjectDataOutput)}
     */
    @Override
    public boolean writeTo(ByteBuffer destination) {
        if (!isStatusSet(ST_TYPE)) {
            if (destination.remaining() < 4) {
                return false;
            }
            destination.putInt(data.type);
            setStatus(ST_TYPE);
        }
        if (!isStatusSet(ST_CLASS_ID)) {
            if (destination.remaining() < 4) {
                return false;
            }
            classId = data.classDefinition == null ? Data.NO_CLASS_ID : data.classDefinition.getClassId();
            destination.putInt(classId);
            if (classId == Data.NO_CLASS_ID) {
                setStatus(ST_FACTORY_ID);
                setStatus(ST_VERSION);
                setStatus(ST_CLASS_DEF_SIZE);
                setStatus(ST_CLASS_DEF);
            }
            setStatus(ST_CLASS_ID);
        }
        if (!isStatusSet(ST_FACTORY_ID)) {
            if (destination.remaining() < 4) {
                return false;
            }
            destination.putInt(data.classDefinition.getFactoryId());
            setStatus(ST_FACTORY_ID);
        }
        if (!isStatusSet(ST_VERSION)) {
            if (destination.remaining() < 4) {
                return false;
            }
            final int version = data.classDefinition.getVersion();
            destination.putInt(version);
            setStatus(ST_VERSION);
        }
        if (!isStatusSet(ST_CLASS_DEF_SIZE)) {
            if (destination.remaining() < 4) {
                return false;
            }
            final BinaryClassDefinition cd = (BinaryClassDefinition) data.classDefinition;
            final byte[] binary = cd.getBinary();
            classDefSize = binary == null ? 0 : binary.length;
            destination.putInt(classDefSize);
            setStatus(ST_CLASS_DEF_SIZE);
            if (classDefSize == 0) {
                setStatus(ST_CLASS_DEF);
            } else {
                buffer = ByteBuffer.wrap(binary);
            }
        }
        if (!isStatusSet(ST_CLASS_DEF)) {
            IOUtil.copyToHeapBuffer(buffer, destination);
            if (buffer.hasRemaining()) {
                return false;
            }
            setStatus(ST_CLASS_DEF);
        }
        if (!isStatusSet(ST_SIZE)) {
            if (destination.remaining() < 4) {
                return false;
            }
            final int size = data.bufferSize();
            destination.putInt(size);
            setStatus(ST_SIZE);
            if (size <= 0) {
                setStatus(ST_VALUE);
            } else {
                buffer = ByteBuffer.wrap(data.buffer);
            }
        }
        if (!isStatusSet(ST_VALUE)) {
            IOUtil.copyToHeapBuffer(buffer, destination);
            if (buffer.hasRemaining()) {
                return false;
            }
            setStatus(ST_VALUE);
        }
        if (!isStatusSet(ST_HASH)) {
            if (destination.remaining() < 4) {
                return false;
            }
            destination.putInt(data.getPartitionHash());
            setStatus(ST_HASH);
        }
        setStatus(ST_ALL);
        return true;
    }

    /**
     * WARNING:
     * <p/>
     * Should be in sync with {@link Data#readData(com.hazelcast.nio.ObjectDataInput)}
     */
    @Override
    public boolean readFrom(ByteBuffer source) {
        if (data == null) {
            data = new Data();
        }
        if (!isStatusSet(ST_TYPE)) {
            if (source.remaining() < 4) {
                return false;
            }
            data.type = source.getInt();
            setStatus(ST_TYPE);
        }
        if (!isStatusSet(ST_CLASS_ID)) {
            if (source.remaining() < 4) {
                return false;
            }
            classId = source.getInt();
            setStatus(ST_CLASS_ID);
            if (classId == Data.NO_CLASS_ID) {
                setStatus(ST_FACTORY_ID);
                setStatus(ST_VERSION);
                setStatus(ST_CLASS_DEF_SIZE);
                setStatus(ST_CLASS_DEF);
            }
        }
        if (!isStatusSet(ST_FACTORY_ID)) {
            if (source.remaining() < 4) {
                return false;
            }
            factoryId = source.getInt();
            setStatus(ST_FACTORY_ID);
        }
        if (!isStatusSet(ST_VERSION)) {
            if (source.remaining() < 4) {
                return false;
            }
            version = source.getInt();
            setStatus(ST_VERSION);
        }
        if (!isStatusSet(ST_CLASS_DEF)) {
            ClassDefinition cd;
            if (!skipClassDef && (cd = context.lookup(factoryId, classId, version)) != null) {
                data.classDefinition = cd;
                skipClassDef = true;
            }
            if (!isStatusSet(ST_CLASS_DEF_SIZE)) {
                if (source.remaining() < 4) {
                    return false;
                }
                classDefSize = source.getInt();
                setStatus(ST_CLASS_DEF_SIZE);
            }
            if (!isStatusSet(ST_CLASS_DEF)) {
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
                setStatus(ST_CLASS_DEF);
            }
        }
        if (!isStatusSet(ST_SIZE)) {
            if (source.remaining() < 4) {
                return false;
            }
            final int size = source.getInt();
            buffer = ByteBuffer.allocate(size);
            setStatus(ST_SIZE);
        }
        if (!isStatusSet(ST_VALUE)) {
            IOUtil.copyToHeapBuffer(source, buffer);
            if (buffer.hasRemaining()) {
                return false;
            }
            buffer.flip();
            data.buffer = buffer.array();
            setStatus(ST_VALUE);
        }
        if (!isStatusSet(ST_HASH)) {
            if (source.remaining() < 4) {
                return false;
            }
            data.partitionHash = source.getInt();
            setStatus(ST_HASH);
        }
        setStatus(ST_ALL);
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
        return isStatusSet(ST_ALL);
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
