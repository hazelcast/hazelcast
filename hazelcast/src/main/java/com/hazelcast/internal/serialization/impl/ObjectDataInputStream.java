/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.nio.DataReader;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.spi.impl.SerializationServiceSupport;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import static com.hazelcast.internal.nio.Bits.NULL_ARRAY_LENGTH;

public class ObjectDataInputStream extends VersionedObjectDataInput
        implements Closeable, DataReader, SerializationServiceSupport {

    private static final int SHORT_MASK = 0xFFFF;

    private final InternalSerializationService serializationService;
    private final DataInputStream dataInput;
    private final ByteOrder byteOrder;

    public ObjectDataInputStream(InputStream in, InternalSerializationService serializationService) {
        this.serializationService = serializationService;
        this.dataInput = new DataInputStream(in);
        this.byteOrder = serializationService.getByteOrder();
    }

    @Override
    public int read() throws IOException {
        return readByte();
    }

    @Override
    public long skip(long n) throws IOException {
        return dataInput.skip(n);
    }

    @Override
    public int available() throws IOException {
        return dataInput.available();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return dataInput.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return dataInput.read(b, off, len);
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        dataInput.readFully(b);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        dataInput.readFully(b, off, len);
    }

    @Override
    public int skipBytes(int n) throws IOException {
        return dataInput.skipBytes(n);
    }

    @Override
    public boolean readBoolean() throws IOException {
        return dataInput.readBoolean();
    }

    @Override
    public byte readByte() throws IOException {
        return dataInput.readByte();
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return dataInput.readUnsignedByte();
    }

    @Override
    public short readShort() throws IOException {
        final short v = dataInput.readShort();
        return bigEndian() ? v : Short.reverseBytes(v);
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return readShort() & SHORT_MASK;
    }

    @Override
    public char readChar() throws IOException {
        final char v = dataInput.readChar();
        return bigEndian() ? v : Character.reverseBytes(v);
    }

    @Override
    public int readInt() throws IOException {
        final int v = dataInput.readInt();
        return bigEndian() ? v : Integer.reverseBytes(v);
    }

    @Override
    public long readLong() throws IOException {
        final long v = dataInput.readLong();
        return bigEndian() ? v : Long.reverseBytes(v);
    }

    @Override
    public float readFloat() throws IOException {
        if (bigEndian()) {
            return dataInput.readFloat();
        } else {
            return Float.intBitsToFloat(readInt());
        }
    }

    @Override
    public double readDouble() throws IOException {
        if (bigEndian()) {
            return dataInput.readDouble();
        } else {
            return Double.longBitsToDouble(readLong());
        }
    }

    @Override
    public byte[] readByteArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            byte[] b = new byte[len];
            readFully(b);
            return b;
        }
        return new byte[0];
    }

    @Override
    public boolean[] readBooleanArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            boolean[] values = new boolean[len];
            for (int i = 0; i < len; i++) {
                values[i] = readBoolean();
            }
            return values;
        }
        return new boolean[0];
    }

    @Override
    public char[] readCharArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            char[] values = new char[len];
            for (int i = 0; i < len; i++) {
                values[i] = readChar();
            }
            return values;
        }
        return new char[0];
    }

    @Override
    public int[] readIntArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            int[] values = new int[len];
            for (int i = 0; i < len; i++) {
                values[i] = readInt();
            }
            return values;
        }
        return new int[0];
    }

    @Override
    public long[] readLongArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            long[] values = new long[len];
            for (int i = 0; i < len; i++) {
                values[i] = readLong();
            }
            return values;
        }
        return new long[0];
    }

    @Override
    public double[] readDoubleArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            double[] values = new double[len];
            for (int i = 0; i < len; i++) {
                values[i] = readDouble();
            }
            return values;
        }
        return new double[0];
    }

    @Override
    public float[] readFloatArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            float[] values = new float[len];
            for (int i = 0; i < len; i++) {
                values[i] = readFloat();
            }
            return values;
        }
        return new float[0];
    }

    @Override
    public short[] readShortArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            short[] values = new short[len];
            for (int i = 0; i < len; i++) {
                values[i] = readShort();
            }
            return values;
        }
        return new short[0];
    }

    @Override
    @Nullable
    @Deprecated
    public String[] readUTFArray() throws IOException {
        return readStringArray();
    }

    @Override
    @Nullable
    public String[] readStringArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            String[] values = new String[len];
            for (int i = 0; i < len; i++) {
                values[i] = readString();
            }
            return values;
        }
        return new String[0];
    }

    @Override
    public String readLine() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Nullable
    @Deprecated
    public String readUTF() throws IOException {
        return readString();
    }

    @Nullable
    @Override
    public String readString() throws IOException {
        int numberOfBytes = readInt();
        if (numberOfBytes == NULL_ARRAY_LENGTH) {
            return null;
        }

        byte[] utf8Bytes = new byte[numberOfBytes];
        dataInput.readFully(utf8Bytes);
        return new String(utf8Bytes, StandardCharsets.UTF_8);
    }

    @Override
    public void close() throws IOException {
        dataInput.close();
    }

    @Override
    public void mark(int readlimit) {
        dataInput.mark(readlimit);
    }

    @Override
    public void reset() throws IOException {
        dataInput.reset();
    }

    @Override
    public boolean markSupported() {
        return dataInput.markSupported();
    }

    @Override
    public <T> T readObject() throws IOException {
        return serializationService.readObject(this);
    }

    // a future optimization would be to skip the construction of the Data object.
    @Override
    public <T> T readDataAsObject() throws IOException {
        Data data = readData();
        return data == null ? null : (T) serializationService.toObject(data);
    }

    @Override
    public <T> T readObject(Class aClass) throws IOException {
        return serializationService.readObject(this, aClass);
    }

    @Override
    public Data readData() throws IOException {
        byte[] bytes = readByteArray();
        return bytes == null ? null : new HeapData(bytes);
    }

    @Override
    public ClassLoader getClassLoader() {
        return serializationService.getClassLoader();
    }

    @Override
    public InternalSerializationService getSerializationService() {
        return serializationService;
    }

    @Override
    public ByteOrder getByteOrder() {
        return byteOrder;
    }

    private boolean bigEndian() {
        return byteOrder == ByteOrder.BIG_ENDIAN;
    }
}
