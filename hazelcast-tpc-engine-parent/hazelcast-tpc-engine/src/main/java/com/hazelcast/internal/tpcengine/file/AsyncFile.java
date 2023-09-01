/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.file;

import com.hazelcast.internal.tpcengine.Eventloop;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.util.IntBiConsumer;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_CLOSE;
import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_FALLOCATE;
import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_FDATASYNC;
import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_FSYNC;
import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_NOP;
import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_OPEN;
import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_READ;
import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_WRITE;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

/**
 * A File where most operations are asynchronous. So operations are submitted
 * asynchronously to the storage device and do not block.
 * <p/>
 * This class isn't thread-safe. It should only be processed by the owning
 * {@link Eventloop}.
 * <p/>
 * If in the future we want to support Virtual Threads, we do not need to
 * introduce a new 'SyncFile'. It would be sufficient to offer blocking operations
 * which acts like scheduling points. This way you can use the API from virtual
 * threads and from platform threads. The platform threads should probably run
 * into an exception when they call a blocking method.
 * <p>
 * The {@link IOBuffer} used for reading/writing be created using the
 * {@link Eventloop#storageAllocator()} because depending on the AsyncFile
 * implementation/configuration, there are special requirements e.g. 4K alignment
 * in case of Direct I/O.
 * <p/>
 * The primary functionality of the AsyncFile is to issue {@link StorageRequest}
 * to the {@link StorageScheduler}. The actual I/O is done by that scheduler.
 */
@SuppressWarnings({"checkstyle:IllegalTokenText", "checkstyle:VisibilityModifier"})
public abstract class AsyncFile {

    public static final int PERMISSIONS_ALL = 0666;

    // todo: in the future we want to have a more
    // OS neutral approach for the options and permissions
    // https://elixir.bootlin.com/linux/latest/source/arch/alpha/include/uapi/asm/fcntl.h#L5
    public static final int O_RDONLY = 00;
    public static final int O_WRONLY = 01;
    public static final int O_RDWR = 02;
    public static final int O_CREAT = 0100;
    public static final int O_TRUNC = 01000;
    public static final int O_DIRECT = 040000;
    public static final int O_SYNC = 04000000;
    public static final int O_NOATIME = 01000000;


    // todo: should be made private and using a varhandle it should be modified
    public int fd = -1;
    protected final Metrics metrics = new Metrics();
    protected final String path;
    protected final StorageScheduler scheduler;
    protected final Eventloop eventloop;

    /**
     * Creates an AsyncFile. The constructor should not be called directly, use the
     * {@link Eventloop#newAsyncFile(String)} to create instances.
     *
     * @param path
     * @param eventloop
     * @param scheduler
     */
    public AsyncFile(String path, Eventloop eventloop, StorageScheduler scheduler) {
        this.path = path;
        this.eventloop = eventloop;
        this.scheduler = scheduler;

        // todo: deal with rejection
        this.eventloop.reactor().files().add(this);
    }

    /**
     * Returns the path to the file.
     *
     * @return the path to the file.
     */
    public final String path() {
        return path;
    }

    /**
     * Returns the {@link Metrics} for this AsyncFile.
     *
     * @return the metrics.
     */
    public final Metrics metrics() {
        return metrics;
    }

    /**
     * Returns the size of the file in bytes.
     * <p/>
     * The AsyncFile must be opened for this method to be called successfully.
     * <p>
     * todo: What happens when the file is not open.
     *
     * @return the size of the file in bytes.
     */
    public abstract long size();

    /**
     * Deletes the AsyncFile.
     *
     * @throws java.io.UncheckedIOException
     */
    public abstract void delete();

    /**
     * Executes a nop asynchronously. This method exists purely for benchmarking
     * purposes and is made for the IORING_OP_NOP.
     * </p>
     * Any other implementation is free to ignore it.
     * <p/>
     * The state of the file is irrelevant to this method.
     *
     * @param callback the callback to call on completion.
     * @throws NullPointerException if callback is <code>null</code>.
     */
    public final void nop(IntBiConsumer<Throwable> callback) {
        checkNotNull(callback, "promise");

        StorageRequest req = scheduler.allocate();
        if (req == null) {
            throw new RuntimeException();
        }

        req.opcode = STR_REQ_OP_NOP;
        req.callback = callback;
        req.file = this;

        scheduler.schedule(req);
    }

    /**
     * Submits an fsync. The sync isn't ordered with respect to any concurrent
     * writes. See {@link #barrierFsync()} for that.
     * <p>
     * todo: probably better to add flag for fdatasync instead of making it a method.
     * <p/>
     * fsync combined with buffered I/O (so using the page cache) is very
     * unreliable. When there are dirty pages in the page cache and an fsync
     * is performed, it will force these dirty pages to be written to disk. If
     * at least one of these writes fails, the page will be marked as failed.
     * As a consequence the fsync will fail, but it will also mark the page as
     * clean and remove the failed state from that page. So if you would perform
     * another fsync, then it very likely would succeed since there are no dirty
     * pages. The problem is that the changes never made it to disk, so that
     * fsync is useless. For more information see: https://danluu.com/fsyncgate/
     *
     * @param callback the callback to call on completion.
     * @throws NullPointerException if callback is <code>null</code>.
     */
    public final void fsync(IntBiConsumer<Throwable> callback) {
        checkNotNull(callback, "callback");

        StorageRequest req = scheduler.allocate();
        if (req == null) {
            throw new RuntimeException();
        }

        req.opcode = STR_REQ_OP_FSYNC;
        req.file = this;
        req.callback = callback;
        scheduler.schedule(req);
    }

    public final void fdatasync(IntBiConsumer<Throwable> callback) {
        checkNotNull(callback, "callback");

        StorageRequest req = scheduler.allocate();
        if (req == null) {
            throw new RuntimeException();
        }

        req.opcode = STR_REQ_OP_FDATASYNC;
        req.file = this;
        req.callback = callback;
        scheduler.schedule(req);
    }

    public final void fallocate(IntBiConsumer<Throwable> callback, int mode, long offset, long length) {
        checkNotNull(callback, "callback");
        checkNotNegative(offset, "offset");
        checkNotNegative(length, "length");

        StorageRequest req = scheduler.allocate();
        if (req == null) {
            throw new RuntimeException();
        }

        req.opcode = STR_REQ_OP_FALLOCATE;
        req.file = this;
        req.callback = callback;
        // The address field is used to store the length of the allocation
        //request.addr = length;
        // the length field is used to store the mode of the allocation
        req.length = mode;
        req.rwFlags = mode;
        req.offset = offset;
        scheduler.schedule(req);
    }

    /**
     * Opens the file with the given flags and permissions.
     *
     * @param callback    the callback to call on completion.
     * @param flags       the flags
     * @param permissions the permissions
     * @throws NullPointerException if callback is <code>null</code>.
     */
    public void open(IntBiConsumer<Throwable> callback, int flags, int permissions) {
        checkNotNull(callback, "callback");

        StorageRequest req = scheduler.allocate();
        if (req == null) {
            throw new RuntimeException();
        }

        req.opcode = STR_REQ_OP_OPEN;
        req.callback = callback;
        req.file = this;
        req.flags = flags;
        req.permissions = permissions;

        scheduler.schedule(req);
    }

    /**
     * Closes the file.
     * <p/>
     * todo: semantics of close on an already closed file.
     * <p/>
     * Just like the linux close, no sync guarantees are provided.
     *
     * @param callback the callback to call on completion.
     * @throws NullPointerException if callback is <code>null</code>.
     */
    public void close(IntBiConsumer<Throwable> callback) {
        checkNotNull(callback, "callback");

        eventloop.reactor().files().remove(this);

        // TODO: Nio open doesn't set the fd.
        if (fd == -1) {
            callback.accept(0, null);
            return;
        }

        StorageRequest req = scheduler.allocate();
        if (req == null) {
            throw new RuntimeException();
        }

        req.opcode = STR_REQ_OP_CLOSE;
        req.file = this;
        req.callback = callback;

        scheduler.schedule(req);
    }

    /**
     * Reads data from a file from some offset.
     * <p/>
     *
     * @param callback the callback to call on completion.
     * @param offset   the offset within the file
     * @param length   the number of bytes to read.
     * @param dst      the IOBuffer to read the data into.
     * @throws NullPointerException if callback is <code>null</code>.
     * @see Eventloop#storageAllocator()
     */
    public final void pread(IntBiConsumer<Throwable> callback, long offset, int length, IOBuffer dst) {
        checkNotNull(callback, "callback");
        checkNotNegative(offset, "offset");
        checkNotNegative(length, "length");
        checkNotNull(dst, "dst");

        StorageRequest req = scheduler.allocate();
        if (req == null) {
            throw new RuntimeException();
        }

        req.opcode = STR_REQ_OP_READ;
        req.file = this;
        req.callback = callback;
        req.buffer = dst;
        req.length = length;
        req.offset = offset;

        scheduler.schedule(req);
    }

    /**
     * Writes data to a file from the given offset.
     * <p/>
     * If Direct I/O is used and the write completes successfully, the write
     * is either in the write buffer of the storage device or on persistent
     * storage of that device.
     * <p/>
     * If the write is in the write buffer of that device, the f(data)sync
     * will guarantee that the write is on persistent storage. On proper
     * enterprise grade SSDs there will be a capacitor or some form of battery
     * on the SSD to guarantee that the write buffer gets written to persistent
     * storage in case of power loss. So in that case, the fsync in theory isn't
     * needed and effectively can be treated as a no-op by the SSD.
     * <p/>
     * If buffered I/O is used (so through the page cache), the write is only
     * in the page cache and not on disk. Only at some point in the future, this
     * write ends up at disk. This makes dealing with I/O errors much more
     * complicated because there is a disconnect between the making the write
     * and completing it (on the page cache) and the actual write to disk. And
     * these problems can bubble up at the fsync.
     *
     * @param callback the callback to call on completion.
     * @param offset   the offset within the file.
     * @param length   the number of bytes to write
     * @param src      the IOBuffer to read the data from.
     * @throws NullPointerException if callback is <code>null</code>.
     * @see Eventloop#storageAllocator()
     */
    public final void pwrite(IntBiConsumer<Throwable> callback, long offset, int length, IOBuffer src) {
        checkNotNull(callback, "callback");
        checkNotNegative(offset, "offset");
        checkNotNegative(length, "length");
        checkNotNull(src, "src");

        StorageRequest req = scheduler.allocate();
        if (req == null) {
            throw new RuntimeException();
        }

        req.opcode = STR_REQ_OP_WRITE;
        req.file = this;
        req.callback = callback;
        req.buffer = src;
        req.length = length;
        req.offset = offset;

        scheduler.schedule(req);
    }

    @Override
    public final String toString() {
        return path;
    }

    /**
     * Contains the metrics for the {@link AsyncFile}.
     * <p/>
     * The metrics should only be updated by the event loop thread, but can be
     * read by any thread.
     */
    @SuppressWarnings("checkstyle:ConstantName")
    public static final class Metrics {

        private static final VarHandle NOPS;
        private static final VarHandle READS;
        private static final VarHandle WRITES;
        private static final VarHandle FSYNCS;
        private static final VarHandle FDATASYNCS;
        private static final VarHandle BYTES_READ;
        private static final VarHandle BYTES_WRITTEN;

        private volatile long nops;
        private volatile long reads;
        private volatile long writes;
        private volatile long fsyncs;
        private volatile long fdatasyncs;
        private volatile long bytesRead;
        private volatile long bytesWritten;

        static {
            try {
                MethodHandles.Lookup l = MethodHandles.lookup();
                NOPS = l.findVarHandle(Metrics.class, "nops", long.class);
                READS = l.findVarHandle(Metrics.class, "reads", long.class);
                WRITES = l.findVarHandle(Metrics.class, "writes", long.class);
                FSYNCS = l.findVarHandle(Metrics.class, "fsyncs", long.class);
                FDATASYNCS = l.findVarHandle(Metrics.class, "fdatasyncs", long.class);
                BYTES_READ = l.findVarHandle(Metrics.class, "bytesRead", long.class);
                BYTES_WRITTEN = l.findVarHandle(Metrics.class, "bytesWritten", long.class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        /**
         * Returns the number of read operations that have been successfully
         * performed on the file.
         */
        public long reads() {
            return (long) READS.getOpaque(this);
        }

        public void incReads() {
            READS.setOpaque(this, (long) READS.getOpaque(this) + 1);
        }

        /**
         * Returns the number of write operations that have been successfully
         * performed on the file.
         */
        public long writes() {
            return (long) WRITES.getOpaque(this);
        }

        public void incWrites() {
            WRITES.setOpaque(this, (long) WRITES.getOpaque(this) + 1);
        }

        /**
         * Returns the number of nop operations that have been successfully
         * performed on the file.
         */
        public long nops() {
            return (long) NOPS.getOpaque(this);
        }

        /**
         * Increases the number of nop operations that have been successfully
         * performed on the file by one.
         */
        public void incNops() {
            NOPS.setOpaque(this, (long) NOPS.getOpaque(this) + 1);
        }

        /**
         * Returns the number of fsyncs that have been successfully called
         * on the file.
         */
        public long fsyncs() {
            return (long) FSYNCS.getOpaque(this);
        }

        public void incFsyncs() {
            FSYNCS.setOpaque(this, (long) FSYNCS.getOpaque(this) + 1);
        }

        /**
         * Returns the number of fdatasyncs that have been successfully
         * called on the file.
         */
        public long fdatasyncs() {
            return (long) FDATASYNCS.getOpaque(this);
        }

        public void incFdatasyncs() {
            FDATASYNCS.setOpaque(this, (long) FDATASYNCS.getOpaque(this) + 1);
        }

        /**
         * Returns the number bytes that have been successfully written to the
         * file.
         */
        public long bytesWritten() {
            return (long) BYTES_WRITTEN.getOpaque(this);
        }

        public void incBytesWritten(int amount) {
            BYTES_WRITTEN.setOpaque(this, (long) BYTES_WRITTEN.getOpaque(this) + amount);
        }

        /**
         * Returns the number of bytes that have been successfully read from the
         * file.
         */
        public long bytesRead() {
            return (long) BYTES_READ.getOpaque(this);
        }

        public void incBytesRead(int amount) {
            BYTES_READ.setOpaque(this, (long) BYTES_READ.getOpaque(this) + amount);
        }
    }
}
