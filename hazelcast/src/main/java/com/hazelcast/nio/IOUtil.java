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

import com.hazelcast.logging.Logger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.DataOutput;
import java.io.DataInput;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;

import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public final class IOUtil {

    public static final byte PRIMITIVE_TYPE_BOOLEAN = 1;
    public static final byte PRIMITIVE_TYPE_BYTE = 2;
    public static final byte PRIMITIVE_TYPE_SHORT = 3;
    public static final byte PRIMITIVE_TYPE_INTEGER = 4;
    public static final byte PRIMITIVE_TYPE_LONG = 5;
    public static final byte PRIMITIVE_TYPE_FLOAT = 6;
    public static final byte PRIMITIVE_TYPE_DOUBLE = 7;
    public static final byte PRIMITIVE_TYPE_UTF = 8;

    private IOUtil() {
    }

    /**
     * This method has a direct dependency on how objects are serialized in
     * {@link com.hazelcast.nio.serialization.DataSerializer}! If the stream
     * format is ever changed this extraction method needs to be changed too!
     */
    public static long extractOperationCallId(Data data, SerializationService serializationService) throws IOException {
        ObjectDataInput input = serializationService.createObjectDataInput(data.getBuffer());
        boolean identified = input.readBoolean();
        if (identified) {
            // read factoryId
            input.readInt();
            // read typeId
            input.readInt();
        } else {
            // read classname
            input.readUTF();
        }

        // read callId
        return input.readLong();
    }

    public static void writeByteArray(ObjectDataOutput out, byte[] value) throws IOException {
        int size = (value == null) ? 0 : value.length;
        out.writeInt(size);
        if (size > 0) {
            out.write(value);
        }
    }

    public static byte[] readByteArray(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        if (size == 0) {
            return null;
        } else {
            byte[] b = new byte[size];
            in.readFully(b);
            return b;
        }
    }

    public static void writeNullableData(ObjectDataOutput out, Data data) throws IOException {
        if (data != null) {
            out.writeBoolean(true);
            data.writeData(out);
        } else {
            // null
            out.writeBoolean(false);
        }
    }

    public static Data readNullableData(ObjectDataInput in) throws IOException {
        final boolean isNotNull = in.readBoolean();
        if (isNotNull) {
            Data data = new Data();
            data.readData(in);
            return data;
        }
        return null;
    }

    public static Data readData(ObjectDataInput in) throws IOException {
        Data data = new Data();
        data.readData(in);
        return data;
    }

    public static ObjectInputStream newObjectInputStream(final ClassLoader classLoader, final InputStream in) throws IOException {
        return new ObjectInputStream(in) {
            protected Class<?> resolveClass(final ObjectStreamClass desc) throws ClassNotFoundException {
                return ClassLoaderUtil.loadClass(classLoader, desc.getName());
            }
        };
    }

    public static OutputStream newOutputStream(final ByteBuffer buf) {
        return new OutputStream() {
            public void write(int b) throws IOException {
                buf.put((byte) b);
            }

            public void write(byte[] bytes, int off, int len) throws IOException {
                buf.put(bytes, off, len);
            }
        };
    }

    public static InputStream newInputStream(final ByteBuffer buf) {
        return new InputStream() {
            public int read() throws IOException {
                if (!buf.hasRemaining()) {
                    return -1;
                }
                return buf.get() & 0xff;
            }

            public int read(byte[] bytes, int off, int len) throws IOException {
                if (!buf.hasRemaining()) {
                    return -1;
                }
                len = Math.min(len, buf.remaining());
                buf.get(bytes, off, len);
                return len;
            }
        };
    }

    public static int copyToHeapBuffer(ByteBuffer src, ByteBuffer dest) {
        if (src == null) {
            return 0;
        }
        int n = Math.min(src.remaining(), dest.remaining());
        if (n > 0) {
            if (n < 16) {
                for (int i = 0; i < n; i++) {
                    dest.put(src.get());
                }
            } else {
                int srcPosition = src.position();
                int destPosition = dest.position();
                System.arraycopy(src.array(), srcPosition, dest.array(), destPosition, n);
                src.position(srcPosition + n);
                dest.position(destPosition + n);
            }
        }
        return n;
    }

    public static int copyToDirectBuffer(ByteBuffer src, ByteBuffer dest) {
        int n = Math.min(src.remaining(), dest.remaining());
        if (n > 0) {
            dest.put(src.array(), src.position(), n);
            src.position(src.position() + n);
        }
        return n;
    }

    public static void writeLongString(DataOutput dos, String str) throws IOException {
        int chunk = 1000;
        int count = str.length() / chunk;
        int remaining = str.length() - (count * chunk);
        dos.writeInt(count + ((remaining > 0) ? 1 : 0));
        for (int i = 0; i < count; i++) {
            dos.writeUTF(str.substring(i * chunk, (i + 1) * chunk));
        }
        if (remaining > 0) {
            dos.writeUTF(str.substring(count * chunk));
        }
    }

    public static String readLongString(DataInput in) throws IOException {
        int count = in.readInt();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(in.readUTF());
        }
        return sb.toString();
    }

    public static byte[] compress(byte[] input) throws IOException {
        Deflater compressor = new Deflater();
        compressor.setLevel(Deflater.BEST_SPEED);
        compressor.setInput(input);
        compressor.finish();
        ByteArrayOutputStream bos = new ByteArrayOutputStream(input.length / 10);
        byte[] buf = new byte[input.length / 10];
        while (!compressor.finished()) {
            int count = compressor.deflate(buf);
            bos.write(buf, 0, count);
        }
        bos.close();
        compressor.end();
        return bos.toByteArray();
    }

    public static byte[] decompress(byte[] compressedData) throws IOException {
        Inflater inflater = new Inflater();
        inflater.setInput(compressedData);
        ByteArrayOutputStream bos = new ByteArrayOutputStream(compressedData.length);
        byte[] buf = new byte[1024];
        while (!inflater.finished()) {
            try {
                int count = inflater.inflate(buf);
                bos.write(buf, 0, count);
            } catch (DataFormatException e) {
                Logger.getLogger(IOUtil.class).finest("Decompression failed", e);
            }
        }
        bos.close();
        inflater.end();
        return bos.toByteArray();
    }


    public static void writeAttributeValue(Object value, ObjectDataOutput out) throws IOException {
        Class<?> type = value.getClass();
        if (type.equals(Boolean.class)) {
            out.writeByte(PRIMITIVE_TYPE_BOOLEAN);
            out.writeBoolean((Boolean) value);
        } else if (type.equals(Byte.class)) {
            out.writeByte(PRIMITIVE_TYPE_BYTE);
            out.writeByte((Byte) value);
        } else if (type.equals(Short.class)) {
            out.writeByte(PRIMITIVE_TYPE_SHORT);
            out.writeShort((Short) value);
        } else if (type.equals(Integer.class)) {
            out.writeByte(PRIMITIVE_TYPE_INTEGER);
            out.writeInt((Integer) value);
        } else if (type.equals(Long.class)) {
            out.writeByte(PRIMITIVE_TYPE_LONG);
            out.writeLong((Long) value);
        } else if (type.equals(Float.class)) {
            out.writeByte(PRIMITIVE_TYPE_FLOAT);
            out.writeFloat((Float) value);
        } else if (type.equals(Double.class)) {
            out.writeByte(PRIMITIVE_TYPE_DOUBLE);
            out.writeDouble((Double) value);
        } else if (type.equals(String.class)) {
            out.writeByte(PRIMITIVE_TYPE_UTF);
            out.writeUTF((String) value);
        } else {
            throw new IllegalStateException("Illegal attribute type id found");
        }
    }

    public static Object readAttributeValue(ObjectDataInput in) throws IOException {
        byte type = in.readByte();
        switch (type) {
            case PRIMITIVE_TYPE_BOOLEAN:
                return in.readBoolean();
            case PRIMITIVE_TYPE_BYTE:
                return in.readByte();
            case PRIMITIVE_TYPE_SHORT:
                return in.readShort();
            case PRIMITIVE_TYPE_INTEGER:
                return in.readInt();
            case PRIMITIVE_TYPE_LONG:
                return in.readLong();
            case PRIMITIVE_TYPE_FLOAT:
                return in.readFloat();
            case PRIMITIVE_TYPE_DOUBLE:
                return in.readDouble();
            case PRIMITIVE_TYPE_UTF:
                return in.readUTF();
            default:
                throw new IllegalStateException("Illegal attribute type id found");
        }

    }

    /**
     * Closes the Closable quietly. So no exception will be thrown. Can also safely be called with a null value.
     *
     * @param closeable the Closeable to close.
     */
    public static void closeResource(final Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                Logger.getLogger(IOUtil.class).finest("closeResource failed", e);
            }
        }
    }
}
