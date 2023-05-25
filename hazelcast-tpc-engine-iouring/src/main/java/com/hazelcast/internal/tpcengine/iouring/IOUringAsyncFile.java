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
import com.hazelcast.internal.tpcengine.file.AsyncFile;
import com.hazelcast.internal.tpcengine.file.AsyncFileMetrics;
import com.hazelcast.internal.tpcengine.file.StorageDevice;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.iouring.StorageDeviceScheduler.IO;

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
    long newestWriteSeq;
    long[] fences;

    private final IOUringEventloop eventloop;
    private final String path;
    private final StorageDeviceScheduler scheduler;


    // todo: Using path as a string forces creating litter.
    IOUringAsyncFile(String path, IOUringReactor reactor) {
        this.path = path;
        this.eventloop = (IOUringEventloop) reactor.eventloop();

        this.dev = reactor.deviceRegistry.findStorageDevice(path);
        if (dev == null) {
            throw newUncheckedIOException("Could not find storage device for [" + path() + "]");
        }

        StorageDeviceScheduler scheduler = eventloop.deviceSchedulers.get(dev);
        if (scheduler == null) {
            scheduler = new StorageDeviceScheduler(dev, eventloop);
            eventloop.deviceSchedulers.put(dev, scheduler);
        }
        this.scheduler = scheduler;
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
        metrics.incNops();

        IO io = scheduler.allocateIO();
        io.file = this;
        io.opcode = IORING_OP_NOP;
        return scheduler.submit(io);
    }

    @Override
    public Promise<Integer> pread(long offset, int length, IOBuffer dst) {
        final AsyncFileMetrics metrics = this.metrics;
        metrics.incReads();
        metrics.incBytesRead(length);

        IO io = scheduler.allocateIO();
        io.file = this;
        io.opcode = IORING_OP_READ;
        io.buf = dst;
        io.addr = dst.address();
        io.len = length;
        io.offset = offset;
        return scheduler.submit(io);
    }

    @Override
    public Promise<Integer> pwrite(long offset, int length, IOBuffer src) {
        final AsyncFileMetrics metrics = this.metrics;
        metrics.incWrites();
        metrics.incBytesWritten(length);

        IO io = scheduler.allocateIO();
        io.file = this;
        io.opcode = IORING_OP_WRITE;
        io.buf = src;
        io.addr = src.address();
        io.len = length;
        io.offset = offset;

        return scheduler.submit(io);
    }

    @Override
    public Promise<Integer> fsync() {
        metrics.incFsyncs();

        IO io = scheduler.allocateIO();
        io.file = this;
        io.opcode = IORING_OP_FSYNC;
        return scheduler.submit(io);
    }

    @Override
    public Promise<Integer> fdatasync() {
        metrics.incFdatasyncs();

        IO io = scheduler.allocateIO();
        io.file = this;
        io.opcode = IORING_OP_FSYNC;
        // The IOURING_FSYNC_DATASYNC maps to the same position as the rw-flags
        io.rwFlags = IORING_FSYNC_DATASYNC;
        return scheduler.submit(io);
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
        Promise<Integer> p = eventloop.promiseAllocator.allocate();

        // todo: unwanted litter.
        byte[] chars = path.getBytes(StandardCharsets.UTF_8);

        IOBuffer pathBuffer = eventloop.fileIOBufferAllocator().allocate(chars.length + SIZEOF_CHAR);
        pathBuffer.writeBytes(chars);
        // C strings end with \0
        pathBuffer.writeChar('\0');
        pathBuffer.flip();

        IO io = scheduler.allocateIO();
        io.file = this;
        io.opcode = IORING_OP_OPENAT;
        io.rwFlags = flags;
        io.addr = pathBuffer.address();
        io.len = permissions;// len field is used for the permissions

        // https://man.archlinux.org/man/io_uring_enter.2.en
        Promise openat = scheduler.submit(io);

        openat.then((BiConsumer<Integer, Throwable>) (fd, throwable) -> {
            pathBuffer.release();

            if (throwable != null) {
                p.completeWithIOException("Can't open file [" + path + "]", throwable);
                return;
            }

            IOUringAsyncFile.this.fd = fd;
            if (fd < 0) {
                p.completeWithIOException("Can't open file [" + path + "]", null);
            } else {
                p.complete(0);
            }
        });

        return p;
    }

    @Override
    public Promise<Integer> close() {
        IO io = scheduler.allocateIO();
        io.file = this;
        io.opcode = IORING_OP_CLOSE;
        return scheduler.submit(io);
    }

    @Override
    public long size() {
        return Linux.filesize(fd);
    }
}
