/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.annotation.PrivateApi;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import static com.hazelcast.util.EmptyStatement.ignore;
import static java.lang.String.format;

@PrivateApi
@SuppressWarnings("checkstyle:methodcount")
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

    /**
     * Fills a buffer from an {@link InputStream}.
     *
     * @param in     the {@link InputStream} to read from
     * @param buffer the buffer to fill
     * @return {@code true} if the buffer could be filled completely,
     * {@code false} if there was no data in the {@link InputStream}
     * @throws IOException if there was not enough data in the {@link InputStream} to fill the buffer
     */
    public static boolean readFullyOrNothing(InputStream in, byte[] buffer) throws IOException {
        int bytesRead = 0;
        do {
            int count = in.read(buffer, bytesRead, buffer.length - bytesRead);
            if (count < 0) {
                if (bytesRead == 0) {
                    return false;
                }
                throw new EOFException();
            }
            bytesRead += count;
        } while (bytesRead < buffer.length);
        return true;
    }

    /**
     * Fills a buffer from an {@link InputStream} unless it doesn't contain enough data.
     *
     * @param in     the {@link InputStream} to read from
     * @param buffer the buffer to fill
     * @throws IOException if there was no data or not enough data in the {@link InputStream} to fill the buffer
     */
    public static void readFully(InputStream in, byte[] buffer) throws IOException {
        if (!readFullyOrNothing(in, buffer)) {
            throw new EOFException();
        }
    }

    public static ObjectInputStream newObjectInputStream(final ClassLoader classLoader, InputStream in) throws IOException {
        return new ClassLoaderAwareObjectInputStream(classLoader, in);
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
     * Quietly attempts to close a {@link Closeable} resource, swallowing any exception.
     *
     * @param closeable the resource to close. If {@code null}, no action is taken.
     */
    public static void closeResource(Closeable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (IOException e) {
            Logger.getLogger(IOUtil.class).finest("closeResource failed", e);
        }
    }

    /**
     * Ensures that the file described by the supplied parameter does not exist
     * after the method returns. If the file didn't exist, returns silently.
     * If the file could not be deleted, returns silently.
     * If the file is a directory, its children are recursively deleted.
     */
    public static void deleteQuietly(File f) {
        try {
            delete(f);
        } catch (Exception e) {
            ignore(e);
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

    /**
     * Ensures that the file described by {@code fileNow} is renamed to file described by {@code fileToBe}.
     * First attempts to perform a direct, atomic rename; if that fails, checks whether the target exists,
     * deletes it, and retries. Throws an exception in each case where the rename failed.
     *
     * @param fileNow  describes an existing file
     * @param fileToBe describes the desired pathname for the file
     */
    public static void rename(File fileNow, File fileToBe) {
        if (fileNow.renameTo(fileToBe)) {
            return;
        }
        if (!fileNow.exists()) {
            throw new HazelcastException(format("Failed to rename %s to %s because %s doesn't exist.",
                    fileNow, fileToBe, fileNow));
        }
        if (!fileToBe.exists()) {
            throw new HazelcastException(format("Failed to rename %s to %s even though %s doesn't exist.",
                    fileNow, fileToBe, fileToBe));
        }
        if (!fileToBe.delete()) {
            throw new HazelcastException(format("Failed to rename %s to %s. %s exists and could not be deleted.",
                    fileNow, fileToBe, fileToBe));
        }
        if (!fileNow.renameTo(fileToBe)) {
            throw new HazelcastException(format("Failed to rename %s to %s even after deleting %s.",
                    fileNow, fileToBe, fileToBe));
        }
    }

    public static String toFileName(String name) {
        return name.replaceAll("[:\\\\/*\"?|<>',]", "_");
    }

    /**
     * Concatenates path parts to a single path using the {@link File#separator} where it applies.
     * There is no validation done on the newly formed path.
     *
     * @param parts The path parts that together should form a path
     * @return The path formed using the parts
     * @throws IllegalArgumentException if the parts param is null or empty
     */
    public static String getPath(String... parts) {
        if (parts == null || parts.length == 0) {
            throw new IllegalArgumentException("Parts is null or empty.");
        }

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            builder.append(part);

            boolean hasMore = i < parts.length - 1;
            if (!part.endsWith(File.separator) && hasMore) {
                builder.append(File.separator);
            }
        }

        return builder.toString();
    }

    public static File getFileFromResources(String resourceFileName) {
        try {
            URL resource = IOUtil.class.getClassLoader().getResource(resourceFileName);
            //noinspection ConstantConditions
            return new File(resource.toURI());
        } catch (Exception e) {
            throw new HazelcastException("Could not find resource file " + resourceFileName, e);
        }
    }

    /**
     * Deep copies source to target and creates the target if necessary. Source can be a directory or a file and the target
     * can be a directory or file. If the source is a directory, expects that the target is a directory (or that it doesn't exist)
     * and nests the copied source under the target directory.
     *
     * @param source the source
     * @param target the destination
     * @throws IllegalArgumentException if the source was not found or the source is a directory and the target is a file
     * @throws HazelcastException       if there was any exception while creating directories or copying
     */
    public static void copy(File source, File target) {
        if (!source.exists()) {
            throw new IllegalArgumentException("Source does not exist");
        }
        if (source.isDirectory()) {
            copyDirectory(source, target);
        } else {
            copyFile(source, target, -1);
        }
    }

    /**
     * Copies source file to target and creates the target if necessary. The target can be a directory or file. If the target
     * is a file, nests the new file under the target directory, otherwise copies to the given target.
     *
     * @param source      the source file
     * @param target      the destination file or directory
     * @param sourceCount The maximum number of bytes to be transferred. If negative transfers the entire source file.
     * @throws IllegalArgumentException if the source was not found or the source not a file
     * @throws HazelcastException       if there was any exception while creating directories or copying
     */
    public static void copyFile(File source, File target, long sourceCount) {
        if (!source.exists()) {
            throw new IllegalArgumentException("Source does not exist");
        }
        if (!source.isFile()) {
            throw new IllegalArgumentException("Source is not a file");
        }
        if (!target.exists() && !target.mkdirs()) {
            throw new HazelcastException("Could not create the target directory " + target);
        }
        final File destination = target.isDirectory() ? new File(target, source.getName()) : target;
        FileInputStream in = null;
        FileOutputStream out = null;
        try {
            in = new FileInputStream(source);
            out = new FileOutputStream(destination);
            final FileChannel inChannel = in.getChannel();
            final FileChannel outChannel = out.getChannel();
            final long transferCount = sourceCount > 0 ? sourceCount : inChannel.size();
            inChannel.transferTo(0, transferCount, outChannel);
        } catch (Exception e) {
            throw new HazelcastException("Error occurred while copying", e);
        } finally {
            closeResource(in);
            closeResource(out);
        }
    }

    private static void copyDirectory(File source, File target) {
        if (target.exists() && !target.isDirectory()) {
            throw new IllegalArgumentException("Cannot copy source directory since the target already exists "
                    + "but it is not a directory");
        }
        final File targetSubDir = new File(target, source.getName());
        if (!targetSubDir.exists() && !targetSubDir.mkdirs()) {
            throw new HazelcastException("Could not create the target directory " + target);
        }
        final File[] sourceFiles = source.listFiles();
        if (sourceFiles == null) {
            throw new HazelcastException("Error occurred while listing directory contents for copy");
        }
        for (File file : sourceFiles) {
            copy(file, targetSubDir);
        }
    }

    public static byte[] toByteArray(InputStream is) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        drainTo(is, baos);
        return baos.toByteArray();
    }

    public static void drainTo(InputStream input, OutputStream output) throws IOException {
        byte[] buffer = new byte[1024];
        int n;
        while (-1 != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
        }
    }

    private static final class ClassLoaderAwareObjectInputStream extends ObjectInputStream {

        private final ClassLoader classLoader;

        private ClassLoaderAwareObjectInputStream(final ClassLoader classLoader, final InputStream in) throws IOException {
            super(in);
            this.classLoader = classLoader;
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc) throws ClassNotFoundException {
            return ClassLoaderUtil.loadClass(classLoader, desc.getName());
        }

        @Override
        protected Class<?> resolveProxyClass(String[] interfaces) throws IOException, ClassNotFoundException {
            ClassLoader theClassLoader = getClassLoader();
            if (theClassLoader == null) {
                return super.resolveProxyClass(interfaces);
            }
            ClassLoader nonPublicLoader = null;
            Class<?>[] classObjs = new Class<?>[interfaces.length];
            for (int i = 0; i < interfaces.length; i++) {
                Class<?> cl = ClassLoaderUtil.loadClass(theClassLoader, interfaces[i]);
                if ((cl.getModifiers() & Modifier.PUBLIC) == 0) {
                    if (nonPublicLoader != null) {
                        if (nonPublicLoader != cl.getClassLoader()) {
                            throw new IllegalAccessError("conflicting non-public interface class loaders");
                        }
                    } else {
                        nonPublicLoader = cl.getClassLoader();
                    }
                }
                classObjs[i] = cl;
            }
            try {
                return Proxy.getProxyClass(nonPublicLoader != null ? nonPublicLoader : theClassLoader, classObjs);
            } catch (IllegalArgumentException e) {
                throw new ClassNotFoundException(null, e);
            }
        }

        private ClassLoader getClassLoader() {
            ClassLoader theClassLoader = this.classLoader;
            if (theClassLoader == null) {
                theClassLoader = Thread.currentThread().getContextClassLoader();
            }
            return theClassLoader;
        }
    }
}
