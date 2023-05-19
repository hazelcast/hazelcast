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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_FSYNC_DATASYNC;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_CLOSE;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_FALLOCATE;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_FSYNC;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_NOP;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_OPENAT;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_READ;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_WRITE;
import static com.hazelcast.internal.tpcengine.util.BufferUtil.addressOf;

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
            throw new UncheckedIOException(new IOException("Could not find storage device for [" + path() + "]"));
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
        return scheduler.submit(this, IORING_OP_NOP, 0, 0, 0, 0, 0);
    }

    @Override
    public Promise<Integer> pread(long offset, int length, long dstAddr) {
        final AsyncFileMetrics metrics = this.metrics;
        metrics.incReads();
        metrics.incBytesRead(length);
        return scheduler.submit(this, IORING_OP_READ, 0, 0, dstAddr, length, offset);
    }

    @Override
    public Promise<Integer> pwrite(long offset, int length, long srcAddr) {
        final AsyncFileMetrics metrics = this.metrics;
        metrics.incWrites();
        metrics.incBytesWritten(length);
        return scheduler.submit(this, IORING_OP_WRITE, 0, 0, srcAddr, length, offset);
    }

    @Override
    public Promise<Integer> fsync() {
        metrics.incFsyncs();
        return scheduler.submit(this, IORING_OP_FSYNC, 0, 0, 0, 0, 0);
    }

    @Override
    public Promise<Integer> fdatasync() {
        metrics.incFdatasyncs();
        // The IOURING_FSYNC_DATASYNC maps to the same position as the rw-flags
        // https://unixism.net/loti/ref-liburing/sqe.html
        return scheduler.submit(this, IORING_OP_FSYNC, 0, IORING_FSYNC_DATASYNC, 0, 0, 0);
    }

    @Override
    public Promise<Integer> barrierFsync() {
        return null;
    }

    @Override
    public void writeBarrier() {
        fences[0] = newestWriteSeq;
        //todo: move to next
    }

    @Override
    public void writeWriteBarrier() {
        fences[0] = newestWriteSeq;
        //todo: move to next
        // todo: no difference write fence and write-write fence
    }

    // for mapping see: https://patchwork.kernel.org/project/linux-fsdevel/patch/20191213183632.19441-2-axboe@kernel.dk/
    @Override
    public Promise<Integer> fallocate(int mode, long offset, long len) {
        return scheduler.submit(this, IORING_OP_FALLOCATE, 0, 0, len, mode, offset);
    }

    @Override
    public Promise<Integer> delete() {
        throw new RuntimeException("Not yet implemented");
    }

    @Override
    public Promise<Integer> open(int flags, int permissions) {
        Promise<Integer> f = eventloop.promiseAllocator.allocate();

        // C strings end with \0
        byte[] chars = path.getBytes(StandardCharsets.UTF_8);
        // todo: litter. Can be pooled.
        ByteBuffer pathBuffer = ByteBuffer.allocateDirect(chars.length + 2);
        pathBuffer.put(chars);
        pathBuffer.putChar('\0');
        pathBuffer.flip(); // not needed

        // https://man.archlinux.org/man/io_uring_enter.2.en
        Promise openat = scheduler.submit(
                this,
                IORING_OP_OPENAT,
                0,
                flags,
                addressOf(pathBuffer),
                permissions,                // len field is used for the permissions
                0);
        openat.then((BiConsumer<Integer, Throwable>) (fd, throwable) -> {
            if (throwable != null) {
                f.completeWithIOException("Can't open file [" + path + "]", throwable);
                return;
            }

            IOUringAsyncFile.this.fd = fd;
            if (fd < 0) {
                f.completeWithIOException("Can't open file [" + path + "]", null);
            } else {
                f.complete(0);
            }
        });

        return f;
    }

    @Override
    public Promise<Integer> close() {
        return scheduler.submit(this, IORING_OP_CLOSE, 0, 0, 0, 0, 0);
    }

    @Override
    public long size() {
        return Linux.filesize(fd);
    }
}
