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

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;

public class DataAdapter implements SocketWritable, SocketReadable {

    private static final int ST_TYPE = 1;
    private static final int ST_SIZE = 2;
    private static final int ST_VALUE = 3;
    private static final int ST_HASH = 4;
    private static final int ST_ALL = 5;

    protected Data data;
    protected PortableContext context;

    private short status;
    private ByteBuffer buffer;
    private ClassDefinitionSerializer classDefinitionSerializer;

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

    public boolean writeTo(ByteBuffer destination) {
        if (!isStatusSet(ST_TYPE)) {
            if (destination.remaining() < INT_SIZE_IN_BYTES + 1) {
                return false;
            }
            int type = data.getType();
            destination.putInt(type);

            boolean hasClassDefinition = context.hasClassDefinition(data);
            destination.put((byte) (hasClassDefinition ? 1 : 0));

            if (hasClassDefinition) {
                classDefinitionSerializer = new ClassDefinitionSerializer(data, context);
            }

            setStatus(ST_TYPE);
        }

        if (classDefinitionSerializer != null) {
            if (!classDefinitionSerializer.write(destination)) {
                return false;
            }
        }

        if (!isStatusSet(ST_HASH)) {
            if (destination.remaining() < INT_SIZE_IN_BYTES) {
                return false;
            }
            destination.putInt(data.hasPartitionHash() ? data.getPartitionHash() : 0);
            setStatus(ST_HASH);
        }
        if (!isStatusSet(ST_SIZE)) {
            if (destination.remaining() < INT_SIZE_IN_BYTES) {
                return false;
            }
            final int size = data.dataSize();
            destination.putInt(size);
            setStatus(ST_SIZE);
            if (size <= 0) {
                setStatus(ST_VALUE);
            } else {
                buffer = ByteBuffer.wrap(data.getData());
            }
        }
        if (!isStatusSet(ST_VALUE)) {
            IOUtil.copyToHeapBuffer(buffer, destination);
            if (buffer.hasRemaining()) {
                return false;
            }
            setStatus(ST_VALUE);
        }
        setStatus(ST_ALL);
        return true;
    }

    public boolean readFrom(ByteBuffer source) {
        if (data == null) {
            data = new DefaultData();
        }
        if (!isStatusSet(ST_TYPE)) {
            if (source.remaining() < INT_SIZE_IN_BYTES + 1) {
                return false;
            }
            int type = source.getInt();
            ((DefaultData) data).setType(type);
            setStatus(ST_TYPE);

            boolean hasClassDefinition = source.get() != 0;
            if (hasClassDefinition) {
                classDefinitionSerializer = new ClassDefinitionSerializer(data, context);
            }
        }

        if (classDefinitionSerializer != null) {
            if (!classDefinitionSerializer.read(source)) {
                return false;
            }
        }

        if (!isStatusSet(ST_HASH)) {
            if (source.remaining() < INT_SIZE_IN_BYTES) {
                return false;
            }
            ((DefaultData) data).setPartitionHash(source.getInt());
            setStatus(ST_HASH);
        }
        if (!isStatusSet(ST_SIZE)) {
            if (source.remaining() < INT_SIZE_IN_BYTES) {
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
            ((DefaultData) data).setData(buffer.array());
            setStatus(ST_VALUE);
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
        data = null;
        status = 0;
        classDefinitionSerializer = null;
    }

    public static int getDataSize(Data data, PortableContext context) {
        // type
        int total = INT_SIZE_IN_BYTES;
        // class def flag
        total += 1;

        if (context.hasClassDefinition(data)) {
            ClassDefinition[] classDefinitions = context.getClassDefinitions(data);
            if (classDefinitions == null || classDefinitions.length == 0) {
                throw new HazelcastSerializationException("ClassDefinition could not be found!");
            }
            // class definitions count
            total += INT_SIZE_IN_BYTES;

            for (ClassDefinition classDef : classDefinitions) {
                // classDefinition-classId
                total += INT_SIZE_IN_BYTES;
                // classDefinition-factory-id
                total += INT_SIZE_IN_BYTES;
                // classDefinition-version
                total += INT_SIZE_IN_BYTES;
                // classDefinition-binary-length
                total += INT_SIZE_IN_BYTES;
                byte[] bytes = ((BinaryClassDefinition) classDef).getBinary();
                // classDefinition-binary
                total += bytes.length;
            }
        }

        // partition-hash
        total += INT_SIZE_IN_BYTES;
        // data-size
        total += INT_SIZE_IN_BYTES;
        // data
        total += data.dataSize();
        return total;
    }
}
