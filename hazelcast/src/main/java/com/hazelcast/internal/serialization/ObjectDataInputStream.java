/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.UTFEncoderDecoder;
import com.hazelcast.nio.serialization.Data;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteOrder;

public class ObjectDataInputStream extends InputStream implements ObjectDataInput, Closeable {

    private static final int UTF_BUFFER_SIZE = 1024;
    private final InternalSerializationService serializationService;
    private final DataInputStream dataInput;
    private final ByteOrder byteOrder;

    private byte[] utfBuffer;

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
        return readShort();
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
        if (len > 0) {
            byte[] b = new byte[len];
            readFully(b);
            return b;
        }
        return new byte[0];
    }

    @Override
    public char[] readCharArray() throws IOException {
        int len = readInt();
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
        if (len > 0) {
            short[] values = new short[len];
            for (int i = 0; i < len; i++) {
                values[i] = readShort();
            }
            return values;
        }
        return new short[0];
    }

    @Deprecated
    public String readLine() throws IOException {
        return dataInput.readLine();
    }

    @Override
    public String readUTF() throws IOException {
        if (utfBuffer == null) {
            utfBuffer = new byte[UTF_BUFFER_SIZE];
        }
        return UTFEncoderDecoder.readUTF(this, utfBuffer);
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
    public Object readObject() throws IOException {
        return serializationService.readObject(this);
    }

    @Override
    public Data readData() throws IOException {
        return serializationService.readData(this);
    }

    @Override
    public ClassLoader getClassLoader() {
        return serializationService.getClassLoader();
    }

    @Override
    public ByteOrder getByteOrder() {
        return byteOrder;
    }

    private boolean bigEndian() {
        return byteOrder == ByteOrder.BIG_ENDIAN;
    }
}
