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


import com.hazelcast.internal.tpcengine.util.IntPromise;
import com.hazelcast.internal.tpcengine.util.IntPromiseAllocator;
import com.hazelcast.internal.tpcengine.file.AsyncFile;
import com.hazelcast.internal.tpcengine.file.BlockDevice;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.iouring.BlockRequestScheduler.BlockRequest;

import java.nio.charset.StandardCharsets;

import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_FSYNC_DATASYNC;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_CLOSE;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_FALLOCATE;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_FSYNC;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_NOP;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_OPENAT;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_READ;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_WRITE;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_CHAR;
import static com.hazelcast.internal.tpcengine.util.ExceptionUtil.newUncheckedIOException;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNegative;

/**
 * IOUring implementation of the {@link AsyncFile}.
 */
@SuppressWarnings({"checkstyle:TrailingComment"})
public final class IOUringAsyncFile extends AsyncFile {
    BlockDevice dev;
    int fd = -1;

    private final IOUringEventloop eventloop;
    private final String path;
    private final BlockRequestScheduler blockRequestScheduler;
    private final IntPromiseAllocator intPromiseAllocator;

    // todo: Using path as a string forces creating litter.
    IOUringAsyncFile(String path, IOUringReactor reactor) {
        this.path = path;
        this.eventloop = (IOUringEventloop) reactor.eventloop();
        this.intPromiseAllocator = eventloop.intPromiseAllocator();
        this.dev = reactor.blockDeviceRegistry.findStorageDevice(path);
        if (dev == null) {
            throw newUncheckedIOException("Could not find storage device for [" + path() + "]");
        }

        BlockRequestScheduler blockRequestScheduler = eventloop.deviceSchedulers.get(dev);
        if (blockRequestScheduler == null) {
            blockRequestScheduler = new BlockRequestScheduler(dev, eventloop);
            eventloop.deviceSchedulers.put(dev, blockRequestScheduler);
        }
        this.blockRequestScheduler = blockRequestScheduler;
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
    public IntPromise nop() {
        IntPromise promise = intPromiseAllocator.allocate();

        BlockRequest request = blockRequestScheduler.reserve();
        if (request == null) {
            return failOnOverload(promise);
        }
        metrics.incNops();

        request.promise = promise;
        request.file = this;
        request.opcode = IORING_OP_NOP;

        blockRequestScheduler.submit(request);
        return promise;
    }

    private static IntPromise failOnOverload(IntPromise promise) {

//        promise.completeWithIOException(
//                "Overload. Max concurrent operations " + maxConcurrent + " dev: [" + dev.path() + "]", null);

        promise.completeWithIOException("No more IO available ", null);
        return promise;
    }

    @Override
    public IntPromise pread(long offset, int length, IOBuffer dst) {
        checkNotNegative(offset, "offset");
        checkNotNegative(length, "length");

        IntPromise promise = intPromiseAllocator.allocate();

        BlockRequest request = blockRequestScheduler.reserve();
        if (request == null) {
            return failOnOverload(promise);
        }

        request.file = this;
        request.promise = promise;
        request.opcode = IORING_OP_READ;
        request.buf = dst;
        request.addr = dst.address();
        request.len = length;
        request.offset = offset;

        blockRequestScheduler.submit(request);

        return promise;
    }

    @Override
    public IntPromise pwrite(long offset, int length, IOBuffer src) {
        checkNotNegative(offset, "offset");
        checkNotNegative(length, "length");

        IntPromise promise = intPromiseAllocator.allocate();
        BlockRequest request = blockRequestScheduler.reserve();
        if (request == null) {
            return failOnOverload(promise);
        }

        request.file = this;
        request.promise = promise;
        request.opcode = IORING_OP_WRITE;
        request.buf = src;
        request.addr = src.address();
        request.len = length;
        request.offset = offset;

        blockRequestScheduler.submit(request);
        return promise;
    }

    @Override
    public IntPromise fsync() {
        IntPromise promise = intPromiseAllocator.allocate();
        BlockRequest request = blockRequestScheduler.reserve();
        if (request == null) {
            return failOnOverload(promise);
        }

        request.file = this;
        request.promise = promise;
        request.opcode = IORING_OP_FSYNC;
        blockRequestScheduler.submit(request);
        return promise;
    }

    @Override
    public IntPromise fdatasync() {
        IntPromise promise = intPromiseAllocator.allocate();
        BlockRequest request = blockRequestScheduler.reserve();
        if (request == null) {
            return failOnOverload(promise);
        }

        request.file = this;
        request.promise = promise;
        request.opcode = IORING_OP_FSYNC;
        // The IOURING_FSYNC_DATASYNC maps to the same position as the rw-flags
        request.rwFlags = IORING_FSYNC_DATASYNC;
        blockRequestScheduler.submit(request);
        return promise;
    }

    @Override
    public IntPromise barrierFsync() {
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
    public IntPromise fallocate(int mode, long offset, long length) {
        checkNotNegative(offset, "offset");
        checkNotNegative(length, "length");

        IntPromise promise = intPromiseAllocator.allocate();
        BlockRequest request = blockRequestScheduler.reserve();
        if (request == null) {
            return failOnOverload(promise);
        }

        request.file = this;
        request.promise = promise;
        request.opcode = IORING_OP_FALLOCATE;
        // The address field is used to store the length of the allocation
        request.addr = length;
        // the length field is used to store the mode of the allocation
        request.len = mode;
        request.rwFlags = mode;
        request.offset = offset;
        blockRequestScheduler.submit(request);
        return promise;
    }

    @Override
    public IntPromise delete() {
        throw new RuntimeException("Not yet implemented");
    }

    @Override
    public IntPromise open(int flags, int permissions) {
        IntPromise promise = intPromiseAllocator.allocate();

        // todo: unwanted litter.
        byte[] chars = path.getBytes(StandardCharsets.UTF_8);

        IOBuffer pathBuffer = eventloop.blockIOBufferAllocator().allocate(chars.length + SIZEOF_CHAR);
        pathBuffer.writeBytes(chars);
        // C strings end with \0
        pathBuffer.writeChar('\0');
        pathBuffer.flip();

        BlockRequest request = blockRequestScheduler.reserve();
        if (request == null) {
            return failOnOverload(promise);
        }

        request.promise = promise;
        request.file = this;
        request.opcode = IORING_OP_OPENAT;
        request.rwFlags = flags;
        request.addr = pathBuffer.address();
        request.buf = pathBuffer;
        request.len = permissions; // len field is used for the permissions

        blockRequestScheduler.submit(request);
        return promise;
    }

    @Override
    public IntPromise close() {
        IntPromise promise = intPromiseAllocator.allocate();
        BlockRequest request = blockRequestScheduler.reserve();
        if (request == null) {
            return failOnOverload(promise);
        }

        request.file = this;
        request.promise = promise;
        request.opcode = IORING_OP_CLOSE;

        blockRequestScheduler.submit(request);
        return promise;
    }

    @Override
    public long size() {
        return Linux.filesize(fd);
    }
}
