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

import com.hazelcast.internal.nio.DataWriter;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.SerializationServiceSupport;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import static com.hazelcast.internal.nio.Bits.NULL_ARRAY_LENGTH;

@SuppressWarnings("checkstyle:methodcount")
public class ObjectDataOutputStream extends VersionedObjectDataOutput
        implements ObjectDataOutput, Closeable, SerializationServiceSupport, DataWriter {

    private final InternalSerializationService serializationService;
    private final DataOutputStream dataOut;
    private final ByteOrder byteOrder;

    public ObjectDataOutputStream(OutputStream outputStream, InternalSerializationService serializationService) {
        this.serializationService = serializationService;
        this.dataOut = new DataOutputStream(outputStream);
        this.byteOrder = serializationService.getByteOrder();
    }

    @Override
    public void write(int b) throws IOException {
        dataOut.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        dataOut.write(b, off, len);
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        dataOut.writeBoolean(v);
    }

    @Override
    public void writeByte(int v) throws IOException {
        dataOut.writeByte(v);
    }

    @Override
    public void writeShort(int v) throws IOException {
        if (bigEndian()) {
            dataOut.writeShort(v);
        } else {
            dataOut.writeShort(Short.reverseBytes((short) v));
        }
    }

    @Override
    public void writeChar(int v) throws IOException {
        if (bigEndian()) {
            dataOut.writeChar(v);
        } else {
            dataOut.writeChar(Character.reverseBytes((char) v));
        }
    }

    @Override
    public void writeInt(int v) throws IOException {
        if (bigEndian()) {
            dataOut.writeInt(v);
        } else {
            dataOut.writeInt(Integer.reverseBytes(v));
        }
    }

    @Override
    public void writeLong(long v) throws IOException {
        if (bigEndian()) {
            dataOut.writeLong(v);
        } else {
            dataOut.writeLong(Long.reverseBytes(v));
        }
    }

    @Override
    public void writeFloat(float v) throws IOException {
        if (bigEndian()) {
            dataOut.writeFloat(v);
        } else {
            writeInt(Float.floatToIntBits(v));
        }
    }

    @Override
    public void writeDouble(double v) throws IOException {
        if (bigEndian()) {
            dataOut.writeDouble(v);
        } else {
            writeLong(Double.doubleToLongBits(v));
        }
    }

    @Override
    public void writeBytes(String s) throws IOException {
        dataOut.writeBytes(s);
    }

    @Override
    public void writeChars(String s) throws IOException {
        int len = s.length();
        for (int i = 0; i < len; i++) {
            int v = s.charAt(i);
            writeChar(v);
        }
    }

    @Override
    public void writeByteArray(byte[] bytes) throws IOException {
        int len = (bytes != null) ? bytes.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            write(bytes);
        }
    }

    @Override
    public void writeBooleanArray(boolean[] booleans) throws IOException {
        int len = booleans != null ? booleans.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (boolean c : booleans) {
                writeBoolean(c);
            }
        }

    }

    @Override
    public void writeCharArray(char[] chars) throws IOException {
        int len = chars != null ? chars.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (char c : chars) {
                writeChar(c);
            }
        }
    }

    @Override
    public void writeIntArray(int[] ints) throws IOException {
        int len = ints != null ? ints.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (int i : ints) {
                writeInt(i);
            }
        }
    }

    @Override
    public void writeLongArray(long[] longs) throws IOException {
        int len = longs != null ? longs.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (long l : longs) {
                writeLong(l);
            }
        }
    }

    @Override
    public void writeDoubleArray(double[] doubles) throws IOException {
        int len = doubles != null ? doubles.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (double d : doubles) {
                writeDouble(d);
            }
        }
    }

    @Override
    public void writeFloatArray(float[] floats) throws IOException {
        int len = floats != null ? floats.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (float f : floats) {
                writeFloat(f);
            }
        }
    }

    @Override
    public void writeShortArray(short[] shorts) throws IOException {
        int len = shorts != null ? shorts.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (short s : shorts) {
                writeShort(s);
            }
        }
    }

    @Override
    @Deprecated
    public void writeUTFArray(@Nullable String[] strings) throws IOException {
        writeStringArray(strings);
    }

    @Override
    public void writeStringArray(@Nullable String[] strings) throws IOException {
        int len = strings != null ? strings.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (String s : strings) {
                writeString(s);
            }
        }
    }

    @Override
    @Deprecated
    public void writeUTF(@Nullable String str) throws IOException {
        writeString(str);
    }

    @Override
    public void writeString(@Nullable String str) throws IOException {
        if (str == null) {
            writeInt(NULL_ARRAY_LENGTH);
            return;
        }

        byte[] utf8Bytes = str.getBytes(StandardCharsets.UTF_8);
        writeInt(utf8Bytes.length);
        write(utf8Bytes);
    }

    @Override
    public void write(byte[] b) throws IOException {
        dataOut.write(b);
    }

    @Override
    public void writeObject(Object object) throws IOException {
        serializationService.writeObject(this, object);
    }

    @Override
    public void writeData(Data data) throws IOException {
        byte[] payload = data != null ? data.toByteArray() : null;
        writeByteArray(payload);
    }

    @Override
    public byte[] toByteArray() {
        return toByteArray(0);
    }

    @Override
    public byte[] toByteArray(int padding) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void flush() throws IOException {
        dataOut.flush();
    }

    @Override
    public void close() throws IOException {
        dataOut.close();
    }

    @Override
    public ByteOrder getByteOrder() {
        return byteOrder;
    }

    @Override
    public SerializationService getSerializationService() {
        return serializationService;
    }

    private boolean bigEndian() {
        return byteOrder == ByteOrder.BIG_ENDIAN;
    }
}
