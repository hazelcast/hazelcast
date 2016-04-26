/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.serialization.Data;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
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

    public static ByteBuffer newByteBuffer(int bufferSize, boolean direct) {
        if (direct) {
            return ByteBuffer.allocateDirect(bufferSize);
        } else {
            return ByteBuffer.allocate(bufferSize);
        }
    }

    /**
     * This method has a direct dependency on how objects are serialized in
     * {@link com.hazelcast.internal.serialization.impl.DataSerializer}! If the stream
     * format is ever changed this extraction method needs to be changed too!
     */
    public static long extractOperationCallId(Data data, InternalSerializationService serializationService) throws IOException {
        ObjectDataInput input = serializationService.createObjectDataInput(data);
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

    public static void writeObject(ObjectDataOutput out, Object object) throws IOException {
        boolean isBinary = object instanceof Data;
        out.writeBoolean(isBinary);
        if (isBinary) {
            out.writeData((Data) object);
        } else {
            out.writeObject(object);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T readObject(ObjectDataInput in) throws IOException {
        boolean isBinary = in.readBoolean();
        if (isBinary) {
            return (T) in.readData();
        }
        return in.readObject();
    }

    public static ObjectInputStream newObjectInputStream(final ClassLoader classLoader, InputStream in) throws IOException {
        return new FilteringObjectInputStream(in) {
            protected Class<?> resolveClass(ObjectStreamClass desc) throws ClassNotFoundException {
                try {
                    // Check deserialization filters
                    super.resolveClass(desc);
                } catch (IOException e) {
                    throw new ClassNotFoundException("Class not resolvable", e);
                }
                return ClassLoaderUtil.loadClass(classLoader, desc.getName());
            }
        };
    }

    public static OutputStream newOutputStream(final ByteBuffer dst) {
        return new OutputStream() {
            public void write(int b) throws IOException {
                dst.put((byte) b);
            }

            public void write(byte[] bytes, int off, int len) throws IOException {
                dst.put(bytes, off, len);
            }
        };
    }

    public static InputStream newInputStream(final ByteBuffer src) {
        return new InputStream() {
            public int read() throws IOException {
                if (!src.hasRemaining()) {
                    return -1;
                }
                return src.get() & 0xff;
            }

            public int read(byte[] bytes, int off, int len) throws IOException {
                if (!src.hasRemaining()) {
                    return -1;
                }
                len = Math.min(len, src.remaining());
                src.get(bytes, off, len);
                return len;
            }
        };
    }

    public static int copyToHeapBuffer(ByteBuffer src, ByteBuffer dst) {
        if (src == null) {
            return 0;
        }
        int n = Math.min(src.remaining(), dst.remaining());
        if (n > 0) {
            if (n < 16) {
                for (int i = 0; i < n; i++) {
                    dst.put(src.get());
                }
            } else {
                int srcPosition = src.position();
                int destPosition = dst.position();
                System.arraycopy(src.array(), srcPosition, dst.array(), destPosition, n);
                src.position(srcPosition + n);
                dst.position(destPosition + n);
            }
        }
        return n;
    }

    public static byte[] compress(byte[] input) throws IOException {
        if (input.length == 0) {
            return new byte[0];
        }
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
        if (compressedData.length == 0) {
            return compressedData;
        }
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
    public static void closeResource(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                Logger.getLogger(IOUtil.class).finest("closeResource failed", e);
            }
        }
    }

    /**
     * Ensures that the file described by the supplied parameter does not exist
     * after the method returns. If the file didn't exist, returns silently.
     * If the file could not be deleted, fails with an exception.
     * If the file is a directory, its children are recursively deleted.
     */
    public static void delete(File f) {
        if (!f.exists()) {
            return;
        }
        File[] subFiles = f.listFiles();
        if (subFiles != null) {
            for (File sf : subFiles) {
                delete(sf);
            }
        }
        if (!f.delete()) {
            throw new HazelcastException("Failed to delete " + f);
        }
    }

    public static String toFileName(String name) {
        return name.replaceAll("[:\\\\/*\"?|<>',]", "_");
    }
}
