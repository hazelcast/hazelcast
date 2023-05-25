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
import com.hazelcast.internal.tpcengine.file.StorageDevice;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.iouring.BlockIOScheduler.BlockIO;

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

/**
 * IOUring implementation of the {@link AsyncFile}.
 */
@SuppressWarnings({"checkstyle:TrailingComment"})
public final class IOUringAsyncFile extends AsyncFile {
    StorageDevice dev;
    int fd=-1;

    private final IOUringEventloop eventloop;
    private final String path;
    private final BlockIOScheduler blockIOScheduler;
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

        BlockIOScheduler blockIOScheduler = eventloop.deviceSchedulers.get(dev);
        if (blockIOScheduler == null) {
            blockIOScheduler = new BlockIOScheduler(dev, eventloop);
            eventloop.deviceSchedulers.put(dev, blockIOScheduler);
        }
        this.blockIOScheduler = blockIOScheduler;
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
        Promise<Integer> promise = promiseAllocator.allocate();

        BlockIO bio = blockIOScheduler.reserve();
        if (bio == null) {
            return failOnOverload(promise);
        }
        metrics.incNops();

        bio.promise = promise;
        bio.file = this;
        bio.opcode = IORING_OP_NOP;

        blockIOScheduler.submit(bio);
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
        Promise<Integer> promise = promiseAllocator.allocate();

        BlockIO bio = blockIOScheduler.reserve();
        if (bio == null) {
            return failOnOverload(promise);
        }

        bio.file = this;
        bio.promise = promise;
        bio.opcode = IORING_OP_READ;
        bio.buf = dst;
        bio.addr = dst.address();
        bio.len = length;
        bio.offset = offset;

        blockIOScheduler.submit(bio);

        return promise;
    }

    @Override
    public Promise<Integer> pwrite(long offset, int length, IOBuffer src) {
        Promise<Integer> promise = promiseAllocator.allocate();
        BlockIO bio = blockIOScheduler.reserve();
        if (bio == null) {
            return failOnOverload(promise);
        }

        bio.file = this;
        bio.promise = promise;
        bio.opcode = IORING_OP_WRITE;
        bio.buf = src;
        bio.addr = src.address();
        bio.len = length;
        bio.offset = offset;

        blockIOScheduler.submit(bio);
        return promise;
    }

    @Override
    public Promise<Integer> fsync() {
        Promise<Integer> promise = promiseAllocator.allocate();
        BlockIO bio = blockIOScheduler.reserve();
        if (bio == null) {
            return failOnOverload(promise);
        }

        bio.file = this;
        bio.promise = promise;
        bio.opcode = IORING_OP_FSYNC;
        blockIOScheduler.submit(bio);
        return promise;
    }

    @Override
    public Promise<Integer> fdatasync() {
        Promise<Integer> promise = promiseAllocator.allocate();
        BlockIO bio = blockIOScheduler.reserve();
        if (bio == null) {
            return failOnOverload(promise);
        }

        bio.file = this;
        bio.promise = promise;
        bio.opcode = IORING_OP_FSYNC;
        // The IOURING_FSYNC_DATASYNC maps to the same position as the rw-flags
        bio.rwFlags = IORING_FSYNC_DATASYNC;
        blockIOScheduler.submit(bio);
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
        Promise<Integer> promise = promiseAllocator.allocate();
        BlockIO bio = blockIOScheduler.reserve();
        if (bio == null) {
            return failOnOverload(promise);
        }

        bio.file = this;
        bio.promise = promise;
        bio.opcode = IORING_OP_FALLOCATE;
        // The IOURING_FSYNC_DATASYNC maps to the same position as the rw-flags
        bio.len = len;
        bio.rwFlags = mode;
        bio.offset = offset;
        blockIOScheduler.submit(bio);
        return promise;

        throw new RuntimeException();
        //return scheduler.submit(this, IORING_OP_FALLOCATE, 0, 0, len,  mode, offset);
    }

    @Override
    public Promise<Integer> delete() {
        throw new RuntimeException("Not yet implemented");
    }

    @Override
    public Promise<Integer> open(int flags, int permissions) {
        Promise<Integer> promise = promiseAllocator.allocate();

        // todo: unwanted litter.
        byte[] chars = path.getBytes(StandardCharsets.UTF_8);

        IOBuffer pathBuffer = eventloop.blockIOBufferAllocator().allocate(chars.length + SIZEOF_CHAR);
        pathBuffer.writeBytes(chars);
        // C strings end with \0
        pathBuffer.writeChar('\0');
        pathBuffer.flip();

        BlockIO bio = blockIOScheduler.reserve();
        if (bio == null) {
            return failOnOverload(promise);
        }

        bio.promise = promise;
        bio.file = this;
        bio.opcode = IORING_OP_OPENAT;
        bio.rwFlags = flags;
        bio.addr = pathBuffer.address();
        bio.buf = pathBuffer;
        bio.len = permissions; // len field is used for the permissions

        blockIOScheduler.submit(bio);
        return promise;
    }

    @Override
    public Promise<Integer> close() {
        Promise<Integer> promise = promiseAllocator.allocate();
        BlockIO bio = blockIOScheduler.reserve();
        if (bio == null) {
            return failOnOverload(promise);
        }

        bio.file = this;
        bio.promise = promise;
        bio.opcode = IORING_OP_CLOSE;

        blockIOScheduler.submit(bio);
        return promise;
    }

    @Override
    public long size() {
        return Linux.filesize(fd);
    }
}
