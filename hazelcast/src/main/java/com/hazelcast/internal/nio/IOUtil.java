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

package com.hazelcast.internal.nio;

import com.hazelcast.config.EndpointConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelOptions;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.ClassNameFilter;

import javax.annotation.Nonnull;
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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Random;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import static com.hazelcast.internal.networking.ChannelOption.DIRECT_BUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_KEEPALIVE;
import static com.hazelcast.internal.networking.ChannelOption.SO_LINGER;
import static com.hazelcast.internal.networking.ChannelOption.SO_RCVBUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_SNDBUF;
import static com.hazelcast.internal.networking.ChannelOption.TCP_NODELAY;
import static com.hazelcast.internal.server.ServerContext.KILO_BYTE;
import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.JVMUtil.upcast;
import static java.lang.String.format;
import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.LinkOption.NOFOLLOW_LINKS;

@SuppressWarnings({ "WeakerAccess", "checkstyle:methodcount", "checkstyle:magicnumber", "checkstyle:classfanoutcomplexity",
        "checkstyle:ClassDataAbstractionCoupling" })
public final class IOUtil {

    private static final ILogger LOGGER = Logger.getLogger(IOUtil.class);
    private static final Random RANDOM = new Random();

    private IOUtil() {
    }

    /**
     * Compacts or clears the buffer depending if bytes are remaining in the byte-buffer.
     *
     * @param bb the ByteBuffer
     */
    public static void compactOrClear(ByteBuffer bb) {
        if (bb.hasRemaining()) {
            bb.compact();
        } else {
            upcast(bb).clear();
        }
    }

    public static ByteBuffer newByteBuffer(int bufferSize, boolean direct) {
        if (direct) {
            return ByteBuffer.allocateDirect(bufferSize);
        } else {
            return ByteBuffer.allocate(bufferSize);
        }
    }


    public static void writeObject(ObjectDataOutput out, Object object) throws IOException {
        boolean isBinary = object instanceof Data;
        out.writeBoolean(isBinary);
        if (isBinary) {
            writeData(out, (Data) object);
        } else {
            out.writeObject(object);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T readObject(ObjectDataInput in) throws IOException {
        boolean isBinary = in.readBoolean();
        if (isBinary) {
            return (T) readData(in);
        }
        return in.readObject();
    }

    public static void writeBigInteger(ObjectDataOutput out, BigInteger value) throws IOException {
        final byte[] bytes = value.toByteArray();
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    public static BigInteger readBigInteger(ObjectDataInput in) throws IOException {
        final byte[] bytes = new byte[in.readInt()];
        in.readFully(bytes);
        return new BigInteger(bytes);
    }

    public static void writeBigDecimal(ObjectDataOutput out, BigDecimal value) throws IOException {
        IOUtil.writeBigInteger(out, value.unscaledValue());
        int scale = value.scale();
        out.writeInt(scale);
    }

    public static BigDecimal readBigDecimal(ObjectDataInput in) throws IOException {
        BigInteger bigInteger = readBigInteger(in);
        int scale = in.readInt();
        return new BigDecimal(bigInteger, scale);
    }

    public static void writeLocalTime(ObjectDataOutput out, LocalTime value) throws IOException {
        int hour = value.getHour();
        int minute = value.getMinute();
        int second = value.getSecond();
        int nano = value.getNano();
        out.writeByte(hour);
        out.writeByte(minute);
        out.writeByte(second);
        out.writeInt(nano);
    }

    public static LocalTime readLocalTime(ObjectDataInput in) throws IOException {
        int hour = in.readByte();
        int minute = in.readByte();
        int second = in.readByte();
        int nano = in.readInt();
        return LocalTime.of(hour, minute, second, nano);
    }

    public static void writeLocalDate(ObjectDataOutput out, LocalDate value) throws IOException {
        int year = value.getYear();
        int monthValue = value.getMonthValue();
        int dayOfMonth = value.getDayOfMonth();
        out.writeInt(year);
        out.writeByte(monthValue);
        out.writeByte(dayOfMonth);
    }

    public static LocalDate readLocalDate(ObjectDataInput in) throws IOException {
        int year = in.readInt();
        int month = in.readByte();
        int dayOfMonth = in.readByte();
        return LocalDate.of(year, month, dayOfMonth);
    }

    public static void writeLocalDateTime(ObjectDataOutput out, LocalDateTime value) throws IOException {
        writeLocalDate(out, value.toLocalDate());
        writeLocalTime(out, value.toLocalTime());
    }

    public static LocalDateTime readLocalDateTime(ObjectDataInput in) throws IOException {
        int year = in.readInt();
        int month = in.readByte();
        int dayOfMonth = in.readByte();

        int hour = in.readByte();
        int minute = in.readByte();
        int second = in.readByte();
        int nano = in.readInt();

        return LocalDateTime.of(year, month, dayOfMonth, hour, minute, second, nano);
    }

    public static void writeOffsetDateTime(ObjectDataOutput out, OffsetDateTime value) throws IOException {
        writeLocalDate(out, value.toLocalDate());
        writeLocalTime(out, value.toLocalTime());

        ZoneOffset offset = value.getOffset();
        int totalSeconds = offset.getTotalSeconds();
        out.writeInt(totalSeconds);
    }

    public static OffsetDateTime readOffsetDateTime(ObjectDataInput in) throws IOException {
        int year = in.readInt();
        int month = in.readByte();
        int dayOfMonth = in.readByte();

        int hour = in.readByte();
        int minute = in.readByte();
        int second = in.readByte();
        int nano = in.readInt();

        int zoneTotalSeconds = in.readInt();
        ZoneOffset zoneOffset = ZoneOffset.ofTotalSeconds(zoneTotalSeconds);
        return OffsetDateTime.of(year, month, dayOfMonth, hour, minute, second, nano, zoneOffset);
    }

    public static void writeData(ObjectDataOutput out, Data data) throws IOException {
        assert out instanceof DataWriter : "out must be an instance of DataWriter";
        ((DataWriter) out).writeData(data);
    }

    public static Data readData(ObjectDataInput in) throws IOException {
        assert in instanceof DataReader : "in must be an instance of DataReader";
        return ((DataReader) in).readData();
    }

    public static <T> T readDataAsObject(ObjectDataInput in) throws IOException {
        assert in instanceof DataReader : "in must be an instance of DataReader";
        return ((DataReader) in).readDataAsObject();
    }

    /**
     * Fills a buffer from an {@link InputStream}. If it doesn't contain any
     * more data, returns {@code false}. If it contains some data, but not
     * enough to fill the buffer, {@link EOFException} is thrown.
     *
     * @param in     the {@link InputStream} to read from
     * @param buffer the buffer to fill
     * @return {@code true} if the buffer was filled completely,
     * {@code false} if there was no data in the {@link InputStream}
     * @throws EOFException if there was some, but not enough, data in the
     *                      {@link InputStream} to fill the buffer
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
     * Fills a buffer from an {@link InputStream}.
     *
     * @param in     the {@link InputStream} to read from
     * @param buffer the buffer to fill
     * @throws EOFException if there was not enough data in the {@link InputStream} to fill the buffer
     */
    public static void readFully(InputStream in, byte[] buffer) throws IOException {
        if (!readFullyOrNothing(in, buffer)) {
            throw new EOFException();
        }
    }

    public static ObjectInputStream newObjectInputStream(final ClassLoader classLoader, ClassNameFilter classFilter,
                                                         InputStream in) throws IOException {
        return new ClassLoaderAwareObjectInputStream(classLoader, classFilter, in);
    }

    public static OutputStream newOutputStream(final ByteBuffer dst) {
        return new OutputStream() {
            public void write(int b) {
                dst.put((byte) b);
            }

            public void write(byte[] bytes, int off, int len) {
                dst.put(bytes, off, len);
            }
        };
    }

    public static InputStream newInputStream(final ByteBuffer src) {
        return new InputStream() {
            public int read() {
                if (!src.hasRemaining()) {
                    return -1;
                }
                return src.get() & 0xff;
            }

            public int read(byte[] bytes, int off, int len) {
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
                upcast(src).position(srcPosition + n);
                upcast(dst).position(destPosition + n);
            }
        }
        return n;
    }

    public static byte[] compress(byte[] input) {
        if (input.length == 0) {
            return new byte[0];
        }
        int len = Math.max(input.length / 10, 10);

        Deflater compressor = new Deflater();
        compressor.setLevel(Deflater.BEST_SPEED);
        compressor.setInput(input);
        compressor.finish();
        ByteArrayOutputStream bos = new ByteArrayOutputStream(len);
        byte[] buf = new byte[len];
        while (!compressor.finished()) {
            int count = compressor.deflate(buf);
            bos.write(buf, 0, count);
        }
        compressor.end();
        return bos.toByteArray();
    }

    public static byte[] decompress(byte[] compressedData) {
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
                LOGGER.finest("Decompression failed", e);
            }
        }
        inflater.end();
        return bos.toByteArray();
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
            LOGGER.finest("closeResource failed", e);
        }
    }

    public static void close(Connection conn, String reason) {
        if (conn == null) {
            return;
        }
        try {
            conn.close(reason, null);
        } catch (Throwable e) {
            LOGGER.finest("closeResource failed", e);
        }
    }

    /**
     * Quietly attempts to close a {@link ServerSocket}, swallowing any exception.
     *
     * @param serverSocket server socket to close. If {@code null}, no action is taken.
     */
    public static void close(ServerSocket serverSocket) {
        if (serverSocket == null) {
            return;
        }
        try {
            serverSocket.close();
        } catch (IOException e) {
            LOGGER.finest("closeResource failed", e);
        }
    }

    /**
     * Quietly attempts to close a {@link Socket}, swallowing any exception.
     *
     * @param socket socket to close. If {@code null}, no action is taken.
     */
    public static void close(Socket socket) {
        if (socket == null) {
            return;
        }
        try {
            socket.close();
        } catch (IOException e) {
            LOGGER.finest("closeResource failed", e);
        }
    }

    /**
     * Ensures that the file described by the supplied parameter does not exist
     * after the method returns. If the file didn't exist, returns silently.
     * If the file could not be deleted, returns silently.
     * If the file is a directory, its children are recursively deleted.
     */
    public static void deleteQuietly(@Nonnull File f) {
        try {
            delete(f.toPath());
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
    public static void delete(@Nonnull File f) {
        delete(f.toPath());
    }

    /**
     * Ensures that the file described by the supplied parameter does not exist
     * after the method returns. If the file didn't exist, returns silently.
     * If the file could not be deleted, fails with an exception.
     * If the file is a directory, its children are recursively deleted.
     */
    public static void delete(@Nonnull Path path) {
        if (!Files.exists(path, NOFOLLOW_LINKS)) {
            return;
        }
        try {
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return CONTINUE;
                }
            });
        } catch (IOException e) {
            throw new HazelcastException("Failed to delete " + path, e);
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

    /**
     * Move or rename a file to a target file. This move method contains a fallback (delete target + atomic move) for cases when
     * the first try fails. It's a NIO-based alternative to the {@link #rename(File, File)} method.
     *
     * @param source path to the file to move
     * @param target path to the target file
     */
    public static void move(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            LOGGER.finest("File move failed. Fallbacking to delete&move.", e);
            Files.deleteIfExists(target);
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        }
    }

    /**
     * Move or rename a file to a target file with retries within the given timeout.
     * Retrying can be beneficial on some systems (e.g. Windows), where file locking may behave non-intuitively.
     *
     * @param source path to the file to move
     * @param target path to the target file
     */
    public static void moveWithTimeout(Path source, Path target, Duration duration) {
        long endTime = System.nanoTime() + duration.toNanos();
        IOException lastException = null;
        do {
            try {
                move(source, target);
            } catch (IOException e) {
                lastException = e;
                LOGGER.finest("File move failed", e);
            }
            if (!Files.exists(source)) {
                return;
            }
            try {
                //random delay up to half a second
                Thread.sleep(RANDOM.nextInt(500));
            } catch (InterruptedException e) {
                ignore(e);
            }
        } while (System.nanoTime() - endTime < 0);
        throw new HazelcastException("File move timed out.", lastException);
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

    public static InputStream getFileFromResourcesAsStream(String resourceFileName) {
        try {
            return IOUtil.class.getClassLoader().getResourceAsStream(resourceFileName);
        } catch (Exception e) {
            throw new HazelcastException("Could not find resource file " + resourceFileName, e);
        }
    }

    /**
     * Simulates a Linux {@code touch} command, by setting the last modified time of given file.
     *
     * @param file the file to touch
     */
    public static void touch(File file) {
        FileOutputStream fos = null;
        try {
            if (!file.exists()) {
                fos = new FileOutputStream(file);
            }
            if (!file.setLastModified(System.currentTimeMillis())) {
                throw new HazelcastException("Could not touch file " + file.getAbsolutePath());
            }
        } catch (IOException e) {
            throw rethrow(e);
        } finally {
            closeResource(fos);
        }
    }

    /**
     * Deep copies source to target and creates the target if necessary.
     * <p>
     * The source can be a directory or a file and the target can be a directory or file.
     * <p>
     * If the source is a directory, it expects that the target is a directory (or that it doesn't exist)
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
     * Deep copies source to target. If target doesn't exist, this will fail with {@link HazelcastException}.
     * <p>
     * The source is only accessed here, but not managed. It's the responsibility of the caller to release
     * any resources held by the source.
     *
     * @param source the source
     * @param target the destination
     * @throws HazelcastException if the target doesn't exist
     */
    public static void copy(InputStream source, File target) {
        if (!target.exists()) {
            throw new HazelcastException("The target file doesn't exist " + target.getAbsolutePath());
        }

        FileOutputStream out = null;
        try {
            out = new FileOutputStream(target);
            byte[] buff = new byte[8192];

            int length;
            while ((length = source.read(buff)) > 0) {
                out.write(buff, 0, length);
            }
        } catch (Exception e) {
            throw new HazelcastException("Error occurred while copying InputStream", e);
        } finally {
            closeResource(out);
        }
    }

    /**
     * Copies source file to target and creates the target if necessary. The target can be a directory or file. If the target
     * is a file, nests the new file under the target directory, otherwise copies to the given target.
     *
     * @param source      the source file
     * @param target      the destination file or directory
     * @param sourceCount the maximum number of bytes to be transferred (if negative transfers the entire source file)
     * @throws IllegalArgumentException if the source was not found or the source not a file
     * @throws HazelcastException       if there was any exception while creating directories or copying
     */
    public static void copyFile(File source, File target, long sourceCount) {
        if (!source.exists()) {
            throw new IllegalArgumentException("Source does not exist " + source.getAbsolutePath());
        }
        if (!source.isFile()) {
            throw new IllegalArgumentException("Source is not a file " + source.getAbsolutePath());
        }
        if (!target.exists() && !target.mkdirs()) {
            throw new HazelcastException("Could not create the target directory " + target.getAbsolutePath());
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
            throw new HazelcastException("Error occurred while copying file", e);
        } finally {
            closeResource(in);
            closeResource(out);
        }
    }

    private static void copyDirectory(File source, File target) {
        if (target.exists() && !target.isDirectory()) {
            throw new IllegalArgumentException("Cannot copy source directory since the target already exists,"
                    + " but it is not a directory");
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
        ByteArrayOutputStream os = null;
        try {
            os = new ByteArrayOutputStream();
            drainTo(is, os);
            return os.toByteArray();
        } finally {
            closeResource(os);
        }
    }

    public static void drainTo(InputStream input, OutputStream output) throws IOException {
        byte[] buffer = new byte[1024];
        int n;
        while (-1 != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
        }
    }

    /**
     * Writes {@code len} bytes from the given input stream to the given output stream.
     *
     * @param input  the input stream
     * @param output the output stream
     * @param len    the number of bytes to write
     * @throws IOException if there are not enough bytes in the input stream, or if there is any other IO error.
     */
    public static void drainTo(InputStream input, OutputStream output, int len) throws IOException {
        byte[] buffer = new byte[1024];
        int remaining = len;
        while (remaining > 0) {
            int n = input.read(buffer, 0, Math.min(buffer.length, remaining));
            if (n == -1) {
                throw new IOException("Not enough bytes in the input stream");
            }
            output.write(buffer, 0, n);
            remaining -= n;
        }
    }

    /**
     * Writes maximum {@code limit} bytes from the given input stream to the given output stream.
     *
     * @param input  the input stream
     * @param output the output stream
     * @param limit  the maximum number of bytes to write
     * @throws IOException if there is any other IO error.
     */
    public static void drainToLimited(InputStream input, OutputStream output, int limit) throws IOException {
        byte[] buffer = new byte[1024];
        int remaining = limit;
        while (remaining > 0) {
            int n = input.read(buffer, 0, Math.min(buffer.length, remaining));
            if (n < 1) {
                return;
            }

            output.write(buffer, 0, n);
            remaining -= n;
        }
    }

    /**
     * Creates a debug String for te given ByteBuffer. Useful when debugging IO.
     * <p>
     * Do not remove even if this method isn't used.
     *
     * @param name       name of the ByteBuffer.
     * @param byteBuffer the ByteBuffer
     * @return the debug String
     */
    public static String toDebugString(String name, ByteBuffer byteBuffer) {
        return name + "(pos:" + byteBuffer.position() + " lim:" + byteBuffer.limit()
                + " remain:" + byteBuffer.remaining() + " cap:" + byteBuffer.capacity() + ")";
    }

    /**
     * Sets configured channel options on given {@link Channel}.
     *
     * @param channel the {@link Channel} on which options will be set
     * @param config  the endpoint configuration
     */
    public static void setChannelOptions(Channel channel, EndpointConfig config) {
        ChannelOptions options = channel.options();
        options.setOption(DIRECT_BUF, config.isSocketBufferDirect())
                .setOption(TCP_NODELAY, config.isSocketTcpNoDelay())
                .setOption(SO_KEEPALIVE, config.isSocketKeepAlive())
                .setOption(SO_SNDBUF, config.getSocketSendBufferSizeKb() * KILO_BYTE)
                .setOption(SO_RCVBUF, config.getSocketRcvBufferSizeKb() * KILO_BYTE)
                .setOption(SO_LINGER, config.getSocketLingerSeconds());
    }

    private static final class ClassLoaderAwareObjectInputStream extends ObjectInputStream {

        private final ClassLoader classLoader;
        private final ClassNameFilter classFilter;

        private ClassLoaderAwareObjectInputStream(final ClassLoader classLoader, ClassNameFilter classFilter,
                                                  final InputStream in) throws IOException {
            super(in);
            this.classLoader = classLoader;
            this.classFilter = classFilter;
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc) throws ClassNotFoundException {
            String name = desc.getName();
            if (classFilter != null) {
                classFilter.filter(name);
            }
            return ClassLoaderUtil.loadClass(classLoader, name);
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
