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

package com.hazelcast.internal.tpc.iouring;


import com.hazelcast.internal.tpc.AsyncFile;
import com.hazelcast.internal.tpc.Promise;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_FSYNC_DATASYNC;
import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_OP_CLOSE;
import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_OP_FALLOCATE;
import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_OP_FSYNC;
import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_OP_NOP;
import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_OP_OPENAT;
import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_OP_READ;
import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_OP_WRITE;
import static com.hazelcast.internal.tpc.util.BufferUtil.addressOf;


/**
 * IOUring implementation of the {@link AsyncFile}.
 */
public final class IOUringAsyncFile extends AsyncFile {

    private final IOUringReactor reactor;
    private final StorageDeviceRegistry storageScheduler;
    private final IOUringEventloop eventloop;
    private final String path;

    StorageDeviceScheduler dev;
    int fd;

    IOUringAsyncFile(String path, IOUringReactor reactor) {
        this.path = path;
        this.reactor = reactor;
        this.eventloop = (IOUringEventloop) reactor.eventloop();
        this.storageScheduler = reactor.storageScheduler;

        this.dev = storageScheduler.findStorageDevice(path);
        if (dev == null) {
            throw new UncheckedIOException(new IOException("Could not find storage device for [" + path() + "]"));
        }
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
        return dev.schedule(this, IORING_OP_NOP, 0, 0, 0, 0, 0);
    }

    @Override
    public Promise<Integer> pread(long offset, int length, long bufferAddress) {
        return dev.schedule(this, IORING_OP_READ, 0, 0, bufferAddress, length, offset);
    }

    @Override
    public Promise<Integer> pwrite(long offset, int length, long bufferAddress) {
        return dev.schedule(this, IORING_OP_WRITE, 0, 0, bufferAddress, length, offset);
    }

    @Override
    public Promise<Integer> pwrite(long offset, int length, long bufferAddress, int flags, int rwFlags) {
        return dev.schedule(this, IORING_OP_WRITE, flags, rwFlags, bufferAddress, length, offset);
    }

    @Override
    public Promise<Integer> fsync() {
        return dev.schedule(this, IORING_OP_FSYNC, 0, 0, 0, 0, 0);
    }

    @Override
    public Promise<Integer> fdatasync() {
        return dev.schedule(this, IORING_OP_FSYNC, IORING_FSYNC_DATASYNC, 0, 0, 0, 0);
    }

    // for mapping see: https://patchwork.kernel.org/project/linux-fsdevel/patch/20191213183632.19441-2-axboe@kernel.dk/
    @Override
    public Promise<Integer> fallocate(int mode, long offset, long len) {
        return dev.schedule(this, IORING_OP_FALLOCATE, 0, 0, len, mode, offset);
    }

    @Override
    public Promise<Integer> delete() {
        throw new RuntimeException("Not yet implemented");
    }

    // todo: this should be taken care of by io_uring and not DirectIoLib because it is blocking.
    @Override
    public Promise<Integer> open(int flags, int permissions) {
        Promise<Integer> f = eventloop.promiseAllocator.allocate();

        // C strings end with \0
        byte[] chars = path.getBytes(StandardCharsets.UTF_8);
        ByteBuffer pathBuffer = ByteBuffer.allocateDirect(chars.length + 2);
        pathBuffer.put(chars);
        pathBuffer.putChar('\0');
        pathBuffer.flip();

        // https://man.archlinux.org/man/io_uring_enter.2.en
        Promise openat = dev.schedule(
                this,
                IORING_OP_OPENAT,
                0,
                flags,
                addressOf(pathBuffer),
                permissions,                // len field is used for the permisssions
                0);
        openat.then(new BiConsumer<Integer, Throwable>() {
            @Override
            public void accept(Integer fd, Throwable throwable) {
                if (throwable != null) {
                    f.completeExceptionally(new RuntimeException(throwable));
                    return;
                }

                IOUringAsyncFile.this.fd = fd;
                if (fd < 0) {
                    f.completeExceptionally(new RuntimeException("Can't open file [" + path + "]"));
                } else {
                    f.complete(0);
                }
            }
        });

        return f;
    }

    @Override
    public Promise<Integer> close() {
        return dev.schedule(this, IORING_OP_CLOSE, 0, 0, 0, 0, 0);
    }
}
