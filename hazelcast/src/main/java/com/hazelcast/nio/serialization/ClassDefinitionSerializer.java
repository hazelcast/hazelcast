/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.Bits;
import com.hazelcast.nio.IOUtil;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.serialization.PortableContext.HEADER_CLASS_OFFSET;
import static com.hazelcast.nio.serialization.PortableContext.HEADER_ENTRY_LENGTH;
import static com.hazelcast.nio.serialization.PortableContext.HEADER_FACTORY_OFFSET;
import static com.hazelcast.nio.serialization.PortableContext.HEADER_VERSION_OFFSET;

/**
 * Serializes/De-serializes ClassDefinitions to/from buffer and streams.
 * <p/>
 * Read/write from/to buffer methods are not thread safe.
 */
public class ClassDefinitionSerializer {

    private static final int CLASS_DEF_HEADER_SIZE = 16;

    private static final int ST_PREPARED = 1;
    private static final int ST_HEADER = 2;
    private static final int ST_DATA = 3;
    private static final int ST_SKIP_DATA = 4;

    // common fields
    private Data data;
    private PortableContext context;
    private byte status;
    private int classDefCount;
    private int classDefIndex;
    private ByteBuffer buffer;

    // write fields
    private ClassDefinition[] classDefinitions;

    // read fields
    private int classDefSize;
    private BinaryClassDefinition classDefProxy;
    private byte[] metadata;

    public ClassDefinitionSerializer(Data data, PortableContext context) {
        this.data = data;
        this.context = context;
    }

    /**
     * Writes a ClassDefinition to a buffer.
     *
     * @param destination     buffer to write ClassDefinition
     * @return true if ClassDefinition is fully written to the buffer,
     * false otherwise
     */
    public boolean write(ByteBuffer destination) {
        if (!isStatusSet(ST_PREPARED)) {
            if (destination.remaining() < INT_SIZE_IN_BYTES) {
                return false;
            }

            classDefinitions = context.getClassDefinitions(data);
            classDefCount = classDefinitions.length;

            destination.putInt(classDefCount);
            setStatus(ST_PREPARED);
        }

        if (!writeAll(destination)) {
            return false;
        }
        return true;
    }

    private boolean writeAll(ByteBuffer destination) {
        for (; classDefIndex < classDefCount; classDefIndex++) {
            ClassDefinitionImpl cd = (ClassDefinitionImpl) classDefinitions[classDefIndex];

            if (!writeHeader(cd, destination)) {
                return false;
            }

            if (!writeData(cd, destination)) {
                return false;
            }

            clearStatus(ST_HEADER);
            clearStatus(ST_DATA);
        }
        return true;
    }

    private boolean writeHeader(ClassDefinitionImpl cd, ByteBuffer destination) {
        if (isStatusSet(ST_HEADER)) {
            return true;
        }

        if (destination.remaining() < CLASS_DEF_HEADER_SIZE) {
            return false;
        }

        destination.putInt(cd.getFactoryId());
        destination.putInt(cd.getClassId());
        destination.putInt(cd.getVersion());

        byte[] binary = cd.getBinary();
        destination.putInt(binary.length);

        setStatus(ST_HEADER);
        return true;
    }

    private boolean writeData(ClassDefinitionImpl cd, ByteBuffer destination) {
        if (isStatusSet(ST_DATA)) {
            return true;
        }

        if (buffer == null) {
            buffer = ByteBuffer.wrap(cd.getBinary());
        }

        if (!flushBuffer(destination)) {
            return false;
        }

        buffer = null;
        setStatus(ST_DATA);
        return true;
    }

    private boolean flushBuffer(ByteBuffer destination) {
        if (buffer.hasRemaining()) {
            IOUtil.copyToHeapBuffer(buffer, destination);
            if (buffer.hasRemaining()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Reads ClassDefinition from a buffer.
     *
     * @param source  buffer to read ClassDefinition from
     * @return true if ClassDefinition is fully read from the buffer,
     * false otherwise
     */
    public boolean read(ByteBuffer source) {
        if (!isStatusSet(ST_PREPARED)) {
            if (source.remaining() < INT_SIZE_IN_BYTES) {
                return false;
            }

            classDefCount = source.getInt();
            metadata = new byte[classDefCount * HEADER_ENTRY_LENGTH];
            ((MutableData) data).setHeader(metadata);

            setStatus(ST_PREPARED);
        }

        if (!readAll(source)) {
            return false;
        }
        return true;
    }

    private boolean readAll(ByteBuffer source) {
        for (; classDefIndex < classDefCount; classDefIndex++) {

            if (!readHeader(source)) {
                return false;
            }

            if (!readData(source)) {
                return false;
            }

            clearStatus(ST_HEADER);
            clearStatus(ST_DATA);
        }
        return true;
    }

    private boolean readHeader(ByteBuffer source) {
        if (isStatusSet(ST_HEADER)) {
            return true;
        }

        if (source.remaining() < CLASS_DEF_HEADER_SIZE) {
            return false;
        }

        int factoryId = source.getInt();
        int classId = source.getInt();
        int version = source.getInt();
        classDefSize = source.getInt();

        boolean bigEndian = context.getByteOrder() == ByteOrder.BIG_ENDIAN;
        Bits.writeInt(metadata, classDefIndex * HEADER_ENTRY_LENGTH + HEADER_FACTORY_OFFSET, factoryId, bigEndian);
        Bits.writeInt(metadata, classDefIndex * HEADER_ENTRY_LENGTH + HEADER_CLASS_OFFSET, classId, bigEndian);
        Bits.writeInt(metadata, classDefIndex * HEADER_ENTRY_LENGTH + HEADER_VERSION_OFFSET, version, bigEndian);

        ClassDefinition cd = context.lookupClassDefinition(factoryId, classId, version);
        if (cd == null) {
            classDefProxy = new BinaryClassDefinitionProxy(factoryId, classId, version);
            clearStatus(ST_SKIP_DATA);
        } else {
            setStatus(ST_SKIP_DATA);
        }
        setStatus(ST_HEADER);
        return true;
    }

    private boolean readData(ByteBuffer source) {
        if (isStatusSet(ST_DATA)) {
            return true;
        }

        if (isStatusSet(ST_SKIP_DATA)) {
            int skip = Math.min(classDefSize, source.remaining());
            source.position(skip + source.position());

            classDefSize -= skip;
            if (classDefSize > 0) {
                return false;
            }
            clearStatus(ST_SKIP_DATA);
        } else {
            if (buffer == null) {
                buffer = ByteBuffer.allocate(classDefSize);
            }

            IOUtil.copyToHeapBuffer(source, buffer);
            if (buffer.hasRemaining()) {
                return false;
            }

            classDefProxy.setBinary(buffer.array());
            context.registerClassDefinition(classDefProxy);
            buffer = null;
            classDefProxy = null;
        }

        setStatus(ST_DATA);
        return true;
    }

    private void setStatus(int bit) {
        status = Bits.setBit(status, bit);
    }

    private void clearStatus(int bit) {
        status = Bits.clearBit(status, bit);
    }

    private boolean isStatusSet(int bit) {
        return Bits.isBitSet(status, bit);
    }
}
