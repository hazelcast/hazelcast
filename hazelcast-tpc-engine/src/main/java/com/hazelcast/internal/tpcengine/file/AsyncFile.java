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
import com.hazelcast.internal.tpcengine.util.IntPromise;
import com.hazelcast.internal.tpcengine.util.IntPromiseAllocator;

import static com.hazelcast.internal.tpcengine.file.BlockRequest.BLK_REQ_OP_CLOSE;
import static com.hazelcast.internal.tpcengine.file.BlockRequest.BLK_REQ_OP_FALLOCATE;
import static com.hazelcast.internal.tpcengine.file.BlockRequest.BLK_REQ_OP_FDATASYNC;
import static com.hazelcast.internal.tpcengine.file.BlockRequest.BLK_REQ_OP_FSYNC;
import static com.hazelcast.internal.tpcengine.file.BlockRequest.BLK_REQ_OP_NOP;
import static com.hazelcast.internal.tpcengine.file.BlockRequest.BLK_REQ_OP_OPEN;
import static com.hazelcast.internal.tpcengine.file.BlockRequest.BLK_REQ_OP_READ;
import static com.hazelcast.internal.tpcengine.file.BlockRequest.BLK_REQ_OP_WRITE;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

/**
 * A File where most operations are asynchronous. So operations are submitted asynchronously
 * to the storage device and do not block.
 * <p/>
 * This class isn't thread-safe. It should only be processed by the owning {@link Eventloop}.
 * <p/>
 * If in the future we want to support Virtual Threads, we do not need to introduce a new
 * 'SyncFile'. It would be sufficient to offer blocking operations which acts like scheduling points.
 * This way you can use the API from virtual threads and from platform threads. The platform threads
 * should probably run into an exception when they call a blocking method.
 * <p>
 * The {@link IOBuffer} used for reading/writing be created using the {@link Eventloop#blockIOBufferAllocator()}
 * because depending on the AsyncFile implementation/configuration, there are special requirements e.g.
 * 4K alignment in case of Direct I/O.
 */
@SuppressWarnings({"checkstyle:IllegalTokenText", "checkstyle:VisibilityModifier"})
public abstract class AsyncFile {

    public static final int PERMISSIONS_ALL = 0666;

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
    protected final AsyncFileMetrics metrics = new AsyncFileMetrics();
    protected final Eventloop eventloop;
    protected final String path;
    protected final BlockRequestScheduler scheduler;
    protected final IntPromiseAllocator promiseAllocator;

    // todo: Using path as a string forces creating litter.
    public AsyncFile(String path, Eventloop eventloop, BlockRequestScheduler scheduler) {
        this.path = path;
        this.eventloop = eventloop;
        this.promiseAllocator = eventloop.intPromiseAllocator();
        this.scheduler = scheduler;
    }

    /**
     * Returns the file descriptor.
     * <p/>
     * If {@link #open(int, int)} hasn't been called, then the value is undefined. todo: perhaps better to return -1?
     *
     * @return the file decriptor.
     */
    public final int fd() {
        return fd;
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
     * Returns the {@link AsyncFileMetrics} for this AsyncFile.
     *
     * @return the metrics.
     */
    public final AsyncFileMetrics metrics() {
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
     * Executes a nop asynchronously. This method exists purely for benchmarking
     * purposes and is made for the IORING_OP_NOP.
     * </p>
     * Any other implementation is free to ignore it.
     * <p/>
     * The state of the file is irrelevant to this method.
     *
     * @return a future.
     */
    public final IntPromise nop() {
        IntPromise promise = promiseAllocator.allocate();

        BlockRequest request = scheduler.reserve();
        if (request == null) {
            return failOnOverload(promise);
        }
        metrics.incNops();

        request.opcode = BLK_REQ_OP_NOP;
        request.promise = promise;
        request.file = this;

        scheduler.submit(request);
        return promise;
    }


    private static IntPromise failOnOverload(IntPromise promise) {

//        promise.completeWithIOException(
//                "Overload. Max concurrent operations " + maxConcurrent + " dev: [" + dev.path() + "]", null);

        promise.completeWithIOException("No more IO available ", null);
        return promise;
    }


    /**
     * Submits an fsync. The sync isn't ordered with respect to any concurrent writes. See
     * {@link #barrierFsync()} for that.
     * <p>
     * todo: probably better to add flag for fdatasync instead of making it a method.
     * <p/>
     * fsync combined with buffered I/O (so using the page cache) is very unreliable. When
     * there are dirty pages in the page cache and an fsync is performed, it will force these
     * dirty pages to be written to disk. If at least one of these writes fails, the page will
     * be marked as failed. As a consequence the fsync will fail, but it will also mark the
     * page as clean and remove the failed state from that page. So if you would perform another
     * fsync, then it very likely would succeed since there are no dirty pages. The problem is
     * that the changes never made it to disk, so that fsync is useless. For more information
     * see: https://danluu.com/fsyncgate/
     *
     * @return
     */
    public final IntPromise fsync() {
        IntPromise promise = promiseAllocator.allocate();
        BlockRequest request = scheduler.reserve();
        if (request == null) {
            return failOnOverload(promise);
        }

        request.opcode = BLK_REQ_OP_FSYNC;
        request.file = this;
        request.promise = promise;
        scheduler.submit(request);
        return promise;
    }

    /**
     * Waits for all prior writes to complete before issuing the fsync. So all earlier writes
     * will be ordered before this fsync but later writes will not be ordered with respect to
     * this fsync.
     * <p>
     * todo: probably better to add flag for fdata sync instead of making it a method.
     * <p>
     * todo: perhaps better to have different barriers?
     * 1) no ordering
     * 2) earlier writes | fsync
     * 3) earlier writes | fsync | later writes
     *
     * @return
     */
    public final IntPromise barrierFsync() {
        throw new UnsupportedOperationException();
    }

    public final void writeBarrier() {
        throw new UnsupportedOperationException();
    }

    public final void writeWriteBarrier() {
        throw new UnsupportedOperationException();
    }

    public final IntPromise fdatasync() {
        IntPromise promise = promiseAllocator.allocate();
        BlockRequest request = scheduler.reserve();
        if (request == null) {
            return failOnOverload(promise);
        }

        request.opcode = BLK_REQ_OP_FDATASYNC;
        request.file = this;
        request.promise = promise;
        scheduler.submit(request);
        return promise;
    }

    public final IntPromise fallocate(int mode, long offset, long length) {
        checkNotNegative(offset, "offset");
        checkNotNegative(length, "length");

        IntPromise promise = promiseAllocator.allocate();
        BlockRequest request = scheduler.reserve();
        if (request == null) {
            return failOnOverload(promise);
        }

        request.opcode = BLK_REQ_OP_FALLOCATE;
        request.file = this;
        request.promise = promise;
        // The address field is used to store the length of the allocation
        //request.addr = length;
        // the length field is used to store the mode of the allocation
        request.length = mode;
        request.rwFlags = mode;
        request.offset = offset;
        scheduler.submit(request);
        return promise;
    }

    public abstract IntPromise delete();

    public IntPromise open(int flags, int permissions) {
        IntPromise promise = promiseAllocator.allocate();

        BlockRequest request = scheduler.reserve();
        if (request == null) {
            return failOnOverload(promise);
        }

        request.opcode = BLK_REQ_OP_OPEN;
        request.promise = promise;
        request.file = this;
        request.flags = flags;
        request.permissions = permissions;

        scheduler.submit(request);
        return promise;
    }

    /**
     * Closes the open file.
     * <p/>
     * todo: semantics of close on an already closed file.
     * <p/>
     * Just like the linux close, no sync guarantees are provided.
     *
     * @return a Promise holding the result of the close.
     */
    public IntPromise close() {
        IntPromise promise = promiseAllocator.allocate();
        BlockRequest request = scheduler.reserve();
        if (request == null) {
            return failOnOverload(promise);
        }

        request.opcode = BLK_REQ_OP_CLOSE;
        request.file = this;
        request.promise = promise;

        scheduler.submit(request);
        return promise;
    }

    /**
     * Reads data from a file from some offset.
     * <p/>
     *
     * @param offset the offset within the file
     * @param length the number of bytes to read.
     * @param dst    the IOBuffer to read the data into.
     * @return a Promise with the response code of the request.
     * @see Eventloop#blockIOBufferAllocator()
     */
    public final IntPromise pread(long offset, int length, IOBuffer dst) {
        checkNotNegative(offset, "offset");
        checkNotNegative(length, "length");
        checkNotNull(dst, "dst");

        IntPromise promise = promiseAllocator.allocate();

        BlockRequest request = scheduler.reserve();
        if (request == null) {
            return failOnOverload(promise);
        }

        request.opcode = BLK_REQ_OP_READ;
        request.file = this;
        request.promise = promise;
        request.buffer = dst;
        request.length = length;
        request.offset = offset;

        scheduler.submit(request);

        return promise;
    }

    /**
     * Writes data to a file from the given offset.
     * <p/>
     * If Direct I/O is used and the write completes successfully, the write is either
     * in the write buffer of the storage device or on persistent storage of that device.
     * <p/>
     * If the write is in the write buffer of that device, the f(data)sync will guarantee
     * that the write is on persistent storage. On proper enterprise grade SSDs there will
     * be a capacitor or some form of battery on the SSD to guarantee that the write buffer
     * gets written to persistent storage in case of power loss. So in that case, the fsync
     * in theory isn't needed and effectively can be treated as a no-op by the SSD.
     * <p/>
     * If buffered I/O is used (so through the page cache), the write is only in the page cache
     * and not on disk. Only at some point in the future, this write ends up at disk. This makes
     * dealing with I/O errors much more complicated because there is a disconnect between the
     * making the write and completing it (on the page cache) and the actual write to disk. And
     * these problems can bubble up at the fsync.
     *
     * @param offset the offset within the file.
     * @param length the number of bytes to write
     * @param src    the IOBuffer to read the data from.
     * @return a Promise with the response code of the request.
     * @see Eventloop#blockIOBufferAllocator()
     */
    public final IntPromise pwrite(long offset, int length, IOBuffer src) {
        checkNotNegative(offset, "offset");
        checkNotNegative(length, "length");
        checkNotNull(src, "src");

        IntPromise promise = promiseAllocator.allocate();
        BlockRequest request = scheduler.reserve();
        if (request == null) {
            return failOnOverload(promise);
        }

        request.opcode = BLK_REQ_OP_WRITE;
        request.file = this;
        request.promise = promise;
        request.buffer = src;
        request.length = length;
        request.offset = offset;

        scheduler.submit(request);
        return promise;
    }


    @Override
    public final String toString() {
        return path;
    }
}
