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

package com.hazelcast.internal.tpcengine.iouring;


import com.hazelcast.internal.tpcengine.Promise;
import com.hazelcast.internal.tpcengine.PromiseAllocator;
import com.hazelcast.internal.tpcengine.file.AsyncFile;
import com.hazelcast.internal.tpcengine.file.AsyncFileMetrics;
import com.hazelcast.internal.tpcengine.file.StorageDevice;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.iouring.FileIOScheduler.FileIO;

import java.nio.charset.StandardCharsets;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_FSYNC_DATASYNC;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_CLOSE;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_FSYNC;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_NOP;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_OPENAT;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_READ;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_WRITE;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_CHAR;
import static com.hazelcast.internal.tpcengine.util.ExceptionUtil.newUncheckedIOException;

/**
 * IOUring implementation of the {@link AsyncFile}.
 */
@SuppressWarnings({"checkstyle:TrailingComment"})
public final class IOUringAsyncFile extends AsyncFile {
    StorageDevice dev;
    int fd;

    private final IOUringEventloop eventloop;
    private final String path;
    private final FileIOScheduler ioScheduler;
    private final PromiseAllocator promiseAllocator;

    // todo: Using path as a string forces creating litter.
    IOUringAsyncFile(String path, IOUringReactor reactor) {
        this.path = path;
        this.eventloop = (IOUringEventloop) reactor.eventloop();
        this.promiseAllocator = eventloop.promiseAllocator;
        this.dev = reactor.deviceRegistry.findStorageDevice(path);
        if (dev == null) {
            throw newUncheckedIOException("Could not find storage device for [" + path() + "]");
        }

        FileIOScheduler ioScheduler = eventloop.deviceSchedulers.get(dev);
        if (ioScheduler == null) {
            ioScheduler = new FileIOScheduler(dev, eventloop);
            eventloop.deviceSchedulers.put(dev, ioScheduler);
        }
        this.ioScheduler = ioScheduler;
    }

    @Override
    public int fd() {
        return fd;
    }

    @Override
    public String path() {
        return path;
    }

    @Override
    public Promise<Integer> nop() {
        Promise promise = promiseAllocator.allocate();

        FileIO io = ioScheduler.reserve();
        if (io == null) {
            return failOnOverload(promise);
        }
        metrics.incNops();

        io.promise = promise;
        io.file = this;
        io.opcode = IORING_OP_NOP;

        ioScheduler.submit(io);
        return promise;
    }

    private static Promise failOnOverload(Promise promise) {

//        promise.completeWithIOException(
//                "Overload. Max concurrent operations " + maxConcurrent + " dev: [" + dev.path() + "]", null);

        promise.completeWithIOException("No more IO available ", null);
        return promise;
    }

    @Override
    public Promise<Integer> pread(long offset, int length, IOBuffer dst) {
        Promise promise = promiseAllocator.allocate();

        FileIO io = ioScheduler.reserve();
        if (io == null) {
            return failOnOverload(promise);
        }
        AsyncFileMetrics metrics = this.metrics;
        metrics.incReads();
        metrics.incBytesRead(length);

        io.file = this;
        io.promise = promise;
        io.opcode = IORING_OP_READ;
        io.buf = dst;
        io.addr = dst.address();
        io.len = length;
        io.offset = offset;

        ioScheduler.submit(io);

        return promise;
    }

    @Override
    public Promise<Integer> pwrite(long offset, int length, IOBuffer src) {
        Promise promise = promiseAllocator.allocate();
        FileIO io = ioScheduler.reserve();
        if (io == null) {
            return failOnOverload(promise);
        }

        final AsyncFileMetrics metrics = this.metrics;
        metrics.incWrites();
        metrics.incBytesWritten(length);

        io.file = this;
        io.promise = promise;
        io.opcode = IORING_OP_WRITE;
        io.buf = src;
        io.addr = src.address();
        io.len = length;
        io.offset = offset;

        ioScheduler.submit(io);
        return promise;
    }

    @Override
    public Promise<Integer> fsync() {
        Promise promise = promiseAllocator.allocate();
        FileIO io = ioScheduler.reserve();
        if (io == null) {
            return failOnOverload(promise);
        }

        io.file = this;
        io.promise = promise;
        io.opcode = IORING_OP_FSYNC;
        metrics.incFsyncs();
        ioScheduler.submit(io);
        return promise;
    }

    @Override
    public Promise<Integer> fdatasync() {
        Promise promise = promiseAllocator.allocate();
        FileIO io = ioScheduler.reserve();
        if (io == null) {
            return failOnOverload(promise);
        }

        metrics.incFdatasyncs();

        io.file = this;
        io.promise = promise;
        io.opcode = IORING_OP_FSYNC;
        // The IOURING_FSYNC_DATASYNC maps to the same position as the rw-flags
        io.rwFlags = IORING_FSYNC_DATASYNC;
        ioScheduler.submit(io);
        return promise;
    }

    @Override
    public Promise<Integer> barrierFsync() {
        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public void writeBarrier() {
        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public void writeWriteBarrier() {
        throw new RuntimeException("Not implemented yet");
    }

    // for mapping see: https://patchwork.kernel.org/project/linux-fsdevel/patch/20191213183632.19441-2-axboe@kernel.dk/
    @Override
    public Promise<Integer> fallocate(int mode, long offset, long len) {
        throw new RuntimeException();
        //return scheduler.submit(this, IORING_OP_FALLOCATE, 0, 0, len,  mode, offset);
    }

    @Override
    public Promise<Integer> delete() {
        throw new RuntimeException("Not yet implemented");
    }

    @Override
    public Promise<Integer> open(int flags, int permissions) {
        Promise<Integer> promise = eventloop.promiseAllocator.allocate();
        Promise<Integer> openat = eventloop.promiseAllocator.allocate();

        // todo: unwanted litter.
        byte[] chars = path.getBytes(StandardCharsets.UTF_8);

        IOBuffer pathBuffer = eventloop.fileIOBufferAllocator().allocate(chars.length + SIZEOF_CHAR);
        pathBuffer.writeBytes(chars);
        // C strings end with \0
        pathBuffer.writeChar('\0');
        pathBuffer.flip();

        FileIO io = ioScheduler.reserve();
        if (io == null) {
            return failOnOverload(promise);
        }

        io.promise = openat;
        io.file = this;
        io.opcode = IORING_OP_OPENAT;
        io.rwFlags = flags;
        io.addr = pathBuffer.address();
        // todo: we are not going to set the pathBuffer on the io, because we do not want the pathBuffer
        // to be updated as part of the openat completion. THe returned value is the fd of the opened file
        // and not the number of bytes read/written
        io.len = permissions;// len field is used for the permissions

        ioScheduler.submit(io);

        openat.then((fd, throwable) -> {
            pathBuffer.release();

            if (throwable != null) {
                promise.completeWithIOException("Can't open file [" + path + "]", throwable);
                return;
            }

            IOUringAsyncFile.this.fd = fd;
            if (fd < 0) {
                promise.completeWithIOException("Can't open file [" + path + "]", null);
            } else {
                promise.complete(0);
            }
        });

        return promise;
    }

    @Override
    public Promise<Integer> close() {
        Promise promise = promiseAllocator.allocate();
        FileIO io = ioScheduler.reserve();
        if (io == null) {
            return failOnOverload(promise);
        }

        io.file = this;
        io.promise = promise;
        io.opcode = IORING_OP_CLOSE;
        ioScheduler.submit(io);
        return promise;
    }

    @Override
    public long size() {
        return Linux.filesize(fd);
    }
}
