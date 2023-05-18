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
import com.hazelcast.internal.tpcengine.Promise;

/**
 * A File that can only be accessed asynchronously. So operations are submitted asynchronously
 * to the storage device and do not block.
 * <p/>
 * This class isn't thread-safe. It should only be processed by the owning {@link Eventloop}.
 * <p/>
 *  If in the future we want to support Virtual Threads, we do not need to introduce a new
 * 'SyncFile'. It would be sufficient to offer blocking operations which acts like scheduling points.
 *  This way you can use the API from virtual threads and from platform threads. The platform threads
 *  should probably run into an exception when they call a blocking method.
 */
public abstract class AsyncFile {

    public static final int PERMISSIONS_ALL = 0666;

    public static final int O_RDONLY = 00;
    public static final int O_WRONLY = 01;
    public static final int O_RDWR = 02;
    public static final int O_CREAT = 0100;
    public static final int O_TRUNC = 01000;
    public static final int O_DIRECT = 040000;
    public static final int O_SYNC = 04000000;
    public static final int O_NOATIME = 01000000;

    protected final AsyncFileMetrics metrics = new AsyncFileMetrics();

    /**
     * Returns the file descriptor.
     * <p/>
     * If {@link #open(int)} hasn't been called, then the value is undefined.
     *
     * @return the file decriptor.
     */
    public abstract int fd();

    /**
     * Returns the {@link AsyncFileMetrics} for this AsyncFile.
     *
     * @return the metrics.
     */
    public AsyncFileMetrics metrics(){
        return metrics;
    }

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
    public abstract Promise<Integer> nop();

    /**
     * Returns the path to the file.
     *
     * @return the path to the file.
     */
    public abstract String path();

    /**
     * Submits an fsync. The sync isn't ordered with respect to any concurrent writes. See
     * {@link #barrierFsync()} for that.
     *
     * todo: probably better to add flag for fdatasync instead of making it a method.
     *
     * @return
     */
    public abstract Promise<Integer> fsync();

    /**
     * Waits for all prior writes to complete before issuing the fsync. So all earlier writes
     * will be ordered before this fsync but later writes will not be ordered with respect to
     * this fsync.
     *
     * todo: probably better to add flag for fdata sync instead of making it a method.
     *
     * todo: perhaps better to have different barriers?
     * 1) no ordering
     * 2) earlier writes | fsync
     * 3) earlier writes | fsync | later writes
     *
     * @return
     */
    public abstract Promise<Integer> barrierFsync();

    public abstract void writeBarrier();

    public abstract void writeWriteBarrier();

    public abstract Promise<Integer> fdatasync();

    public abstract Promise<Integer> fallocate(int mode, long offset, long len);

    public abstract Promise<Integer> delete();

    public abstract Promise<Integer> open(int flags, int permissions);

    /**
     * Closes the open file.
     * <p/>
     * todo: semantics of close on an already closed file.
     * <p/>
     * Just like the linux close, no sync guarantees are provided.
     *
     * @return a Promise holding the result of the close.
     */
    public abstract Promise<Integer> close();

    /**
     * Reads data from a file from some offset.
     * <p>
     * todo:
     * The problem is that we can't just pass any buffer. In case of DIO, the buffer
     * needs to be properly aligned. So if there would be a map.get, you can't just start adding
     * the data after.
     * <p>
     * todo: use an IOBuffer instead of an dstAddr/length. This way can integrate with
     * AsyncFileChannel.
     *
     * @param offset the offset.
     * @param length the number of bytes to read.
     * @return a Fut with the response code of the request.
     */
    public abstract Promise<Integer> pread(long offset, int length, long dstAddr);

    /**
     * Writes data to a file from the given offset.
     * <p>
     * todo: use an IOBuffer instead of an srcAddr/length. This way can integrate with
     * AsyncFileChannel.
     *
     * @param offset
     * @param length
     * @param srcAddr
     * @return a Fut with the response code of the request.
     */
    public abstract Promise<Integer> pwrite(long offset, int length, long srcAddr);

    @Override
    public String toString() {
        return path();
    }
}
