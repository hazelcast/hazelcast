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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public final class Data implements IdentifiedDataSerializable {

    public static final int FACTORY_ID = 0;
    public static final int ID = 0;
    public static final int NO_CLASS_ID = 0; // WARNING: Portable class-id cannot be zero.

    int type = SerializationConstants.CONSTANT_TYPE_DATA;
    ClassDefinition classDefinition = null;
    byte[] buffer = null;
    int partitionHash = 0;

//    transient int hash;

    public Data() {
    }

    public Data(int type, byte[] bytes) {
        this.type = type;
        this.buffer = bytes;
    }

    public void postConstruct(SerializationContext context) {
        if (classDefinition != null && classDefinition instanceof BinaryClassDefinitionProxy) {
            try {
                classDefinition = ((BinaryClassDefinitionProxy) classDefinition).toReal(context);
            } catch (IOException e) {
                throw new HazelcastSerializationException(e);
            }
        }
    }

    /**
     * WARNING:
     * <p/>
     * Should be in sync with {@link DataAdapter#readFrom(java.nio.ByteBuffer)}
     */
    public void readData(ObjectDataInput in) throws IOException {
        type = in.readInt();
        final int classId = in.readInt();
        if (classId != NO_CLASS_ID) {
            final int factoryId = in.readInt();
            final int version = in.readInt();
            SerializationContext context = ((SerializationContextAware) in).getSerializationContext();
            classDefinition = context.lookup(factoryId, classId, version);
            int classDefSize = in.readInt();
            if (classDefinition != null) {
                in.skipBytes(classDefSize);
            } else {
                byte[] classDefBytes = new byte[classDefSize];
                in.readFully(classDefBytes);
                classDefinition = context.createClassDefinition(factoryId, classDefBytes);
            }
        }
        int size = in.readInt();
        if (size > 0) {
            buffer = new byte[size];
            in.readFully(buffer);
        }
        partitionHash = in.readInt();
    }

    /**
     * WARNING:
     * <p/>
     * Should be in sync with {@link DataAdapter#writeTo(java.nio.ByteBuffer)}
     * <p/>
     * {@link #totalSize()} should be updated whenever writeData method is changed.
     */
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(type);
        if (classDefinition != null) {
            out.writeInt(classDefinition.getClassId());
            out.writeInt(classDefinition.getFactoryId());
            out.writeInt(classDefinition.getVersion());
            byte[] classDefBytes = ((BinaryClassDefinition) classDefinition).getBinary();
            out.writeInt(classDefBytes.length);
            out.write(classDefBytes);
        } else {
            out.writeInt(NO_CLASS_ID);
        }
        int size = bufferSize();
        out.writeInt(size);
        if (size > 0) {
            out.write(buffer);
        }
        out.writeInt(getPartitionHash());
    }

    public int bufferSize() {
        return (buffer == null) ? 0 : buffer.length;
    }

    /**
     * Calculates the size of the binary after the Data is serialized.
     * <p/>
     * WARNING:
     * <p/>
     * Should be in sync with {@link #writeData(com.hazelcast.nio.ObjectDataOutput)}
     */
    public int totalSize() {
        int total = 0;
        total += 4; // type
        if (classDefinition != null) {
            total += 4; // classDefinition-classId
            total += 4; // // classDefinition-factory-id
            total += 4; // classDefinition-version
            total += 4; // classDefinition-binary-length
            final byte[] binary = ((BinaryClassDefinition) classDefinition).getBinary();
            total += binary != null ? binary.length : 0; // classDefinition-binary
        } else {
            total += 4; // no-classId
        }
        total += 4; // buffer-size
        total += bufferSize(); // buffer
        total += 4; // partition-hash
        return total;
    }

    public int getHeapCost() {
        int total = 0;
        total += 4; // type
        total += 4; // cd
        total += 16; // buffer array ref (12: array header, 4: length)
        total += bufferSize(); // buffer itself
        total += 4; // partition-hash
        return total;
    }

    @Override
    public int hashCode() {
//        int h = hash;
//        if (h == 0 && bufferSize() > 0) {
//            h = hash = calculateHash(buffer);
//        }
//        return h;
        return calculateHash(buffer);
    }

    private static int calculateHash(final byte[] buffer) {
        if (buffer == null) {
            return 0;
        }
        // FNV (Fowler/Noll/Vo) Hash "1a"
        final int prime = 0x01000193;
        int hash = 0x811c9dc5;
        for (int i = buffer.length - 1; i >= 0; i--) {
            hash = (hash ^ buffer[i]) * prime;
        }
        return hash;
    }

    public int getPartitionHash() {
        int ph = partitionHash;
        if (ph == 0 && bufferSize() > 0) {
            ph = partitionHash = hashCode();
        }
        return ph;
    }

    public int getType() {
        return type;
    }

    public ClassDefinition getClassDefinition() {
        return classDefinition;
    }

    public byte[] getBuffer() {
        return buffer;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Data))
            return false;
        if (this == obj)
            return true;
        Data data = (Data) obj;
        return type == data.type && bufferSize() == data.bufferSize()
                && equals(buffer, data.buffer);
    }

    // Same as Arrays.equals(byte[] a, byte[] a2) but loop order is reversed.
    private static boolean equals(final byte[] data1, final byte[] data2) {
        if (data1 == data2) {
            return true;
        }
        if (data1 == null || data2 == null) {
            return false;
        }
        final int length = data1.length;
        if (data2.length != length) {
            return false;
        }
        for (int i = length - 1; i >= 0; i--) {
            if (data1[i] != data2[i]) {
                return false;
            }
        }
        return true;
    }

    public int getFactoryId() {
        return FACTORY_ID;
    }

    public int getId() {
        return ID;
    }

    public boolean isPortable() {
        return SerializationConstants.CONSTANT_TYPE_PORTABLE == type;
    }

    public boolean isDataSerializable() {
        return SerializationConstants.CONSTANT_TYPE_DATA == type;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Data{");
        sb.append("type=").append(type);
        sb.append(", partitionHash=").append(getPartitionHash());
        sb.append(", bufferSize=").append(bufferSize());
        sb.append(", totalSize=").append(totalSize());
        sb.append('}');
        return sb.toString();
    }
}
