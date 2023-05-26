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

import com.hazelcast.internal.tpcengine.file.AsyncFileMetrics;
import com.hazelcast.internal.tpcengine.file.BlockDevice;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.util.CircularQueue;
import com.hazelcast.internal.tpcengine.util.IntPromise;
import com.hazelcast.internal.tpcengine.util.LongObjectHashMap;
import com.hazelcast.internal.tpcengine.util.SlabAllocator;
import com.hazelcast.internal.tpcengine.util.UnsafeLocator;
import sun.misc.Unsafe;

import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_FSYNC_DATASYNC;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_CLOSE;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_FALLOCATE;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_FSYNC;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_NOP;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_OPENAT;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_READ;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_WRITE;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.opcodeToString;
import static com.hazelcast.internal.tpcengine.iouring.SubmissionQueue.OFFSET_SQE_addr;
import static com.hazelcast.internal.tpcengine.iouring.SubmissionQueue.OFFSET_SQE_fd;
import static com.hazelcast.internal.tpcengine.iouring.SubmissionQueue.OFFSET_SQE_flags;
import static com.hazelcast.internal.tpcengine.iouring.SubmissionQueue.OFFSET_SQE_ioprio;
import static com.hazelcast.internal.tpcengine.iouring.SubmissionQueue.OFFSET_SQE_len;
import static com.hazelcast.internal.tpcengine.iouring.SubmissionQueue.OFFSET_SQE_off;
import static com.hazelcast.internal.tpcengine.iouring.SubmissionQueue.OFFSET_SQE_opcode;
import static com.hazelcast.internal.tpcengine.iouring.SubmissionQueue.OFFSET_SQE_rw_flags;
import static com.hazelcast.internal.tpcengine.iouring.SubmissionQueue.OFFSET_SQE_user_data;
import static com.hazelcast.internal.tpcengine.iouring.SubmissionQueue.SIZEOF_SQE;

/**
 * Todo: The IOScheduler should be a scheduler for all storage devices. Currently it is only for a single device.
 */
public class BlockIOScheduler {

    private static final Unsafe UNSAFE = UnsafeLocator.UNSAFE;

    final String path;
    IOUringEventloop eventloop;
    private final int maxConcurrent;
    private final LongObjectHashMap<CompletionHandler> handlers;
    private final SubmissionQueue sq;
    private final BlockDevice dev;
    private int queueDepth;
    // a queue of IoOps that could not be submitted to io_uring because either
    // io_uring was full or because the max number of concurrent IoOps for that
    // device was exceeded.
    private final CircularQueue<BlockIO> waitQueue;
    private final SlabAllocator<BlockIO> bioAllocator;
    private long count;

    // To prevent intermediate string litter for every IOException the msgBuilder is recycled.
    private final StringBuilder msgBuilder = new StringBuilder();

    public BlockIOScheduler(BlockDevice dev, IOUringEventloop eventloop) {
        this.dev = dev;
        this.path = dev.path();
        this.maxConcurrent = dev.maxConcurrent();
        this.waitQueue = new CircularQueue<>(dev.maxWaiting());
        this.bioAllocator = new SlabAllocator<>(dev.maxWaiting(), BlockIO::new);
        this.eventloop = eventloop;
        this.handlers = eventloop.handlers;
        this.sq = eventloop.sq;
    }

    /**
     * Reserves a single IO. The IO is not submitted to io_uring yet.
     * <p/>
     * If a non null value is returned, it is guaranteed that {@link #submit(BlockIO)} will complete successfully.
     *
     * @return the reserved IO or null if there is no space.
     */
    public BlockIO reserve() {
        //todo: not the right condition
        if (queueDepth >= maxConcurrent) {
            return null;
        }

        return bioAllocator.allocate();
    }

    public void submit(BlockIO bio) {
        if (queueDepth < maxConcurrent) {
            offerIO(bio);
        } else if (!waitQueue.offer(bio)) {
            throw new IllegalStateException("Too many concurrent IOs");
        }
    }

    private void scheduleNext() {
        if (queueDepth < maxConcurrent) {
            BlockIO bio = waitQueue.poll();
            if (bio != null) {
                offerIO(bio);
            }
        }
    }

    private void offerIO(BlockIO bio) {
        long userdata = eventloop.nextTmpHandlerId();
        handlers.put(userdata, bio);

        queueDepth++;

        int index = sq.nextIndex();
        if (index >= 0) {
            //System.out.println("writeSqe:" + index);
            long sqeAddr = sq.sqesAddr + index * SIZEOF_SQE;
            UNSAFE.putByte(sqeAddr + OFFSET_SQE_opcode, bio.opcode);
            UNSAFE.putByte(sqeAddr + OFFSET_SQE_flags, (byte) bio.flags);
            UNSAFE.putShort(sqeAddr + OFFSET_SQE_ioprio, (short) 0);
            UNSAFE.putInt(sqeAddr + OFFSET_SQE_fd, bio.file.fd);
            UNSAFE.putLong(sqeAddr + OFFSET_SQE_off, bio.offset);
            UNSAFE.putLong(sqeAddr + OFFSET_SQE_addr, bio.addr);
            UNSAFE.putInt(sqeAddr + OFFSET_SQE_len, bio.len);
            UNSAFE.putInt(sqeAddr + OFFSET_SQE_rw_flags, bio.rwFlags);
            UNSAFE.putLong(sqeAddr + OFFSET_SQE_user_data, userdata);
            //System.out.println("SubmissionQueue: userdata:" + userData + " index:" + index + " bio:" + opcode);
        } else {
            // can't happen
            // todo: we need to find better solution
            throw new RuntimeException("Could not offer bio: " + bio);
        }
    }

    /**
     * Represents a single I/O operation on a block device. For example a read or write to disk.
     */
    class BlockIO implements CompletionHandler {
        IOBuffer buf;
        IOUringAsyncFile file;
        long offset;
        int len;
        byte opcode;
        int flags;
        int rwFlags;
        long addr;
        IntPromise promise;

        @Override
        public void handle(int res, int flags, long userdata) {
            queueDepth--;
            if (res >= 0) {
                AsyncFileMetrics metrics = file.metrics();
                switch (opcode) {
                    case IORING_OP_READ:
                        buf.incPosition(res);
                        metrics.incReads();
                        metrics.incBytesRead(res);
                        break;
                    case IORING_OP_WRITE:
                        buf.incPosition(res);
                        metrics.incWrites();
                        metrics.incBytesWritten(res);
                        break;
                    case IORING_OP_NOP:
                        metrics.incNops();
                        break;
                    case IORING_OP_FSYNC:
                        if (rwFlags == IORING_FSYNC_DATASYNC) {
                            metrics.incFdatasyncs();
                        } else {
                            metrics.incFsyncs();
                        }
                        break;
                    case IORING_OP_OPENAT:
                        buf.release();
                        file.fd = res;
                        break;
                    case IORING_OP_CLOSE:
                        file.fd = -1;
                        break;
                    case IORING_OP_FALLOCATE:
                        break;
                    default:
                        throw new IllegalStateException("Unknown opcode: " + opcode);
                }
                promise.complete(res);
            } else {
                handleError(res);
            }

            scheduleNext();
            release();
        }

        private void handleError(int res) {
            msgBuilder.setLength(0);
            String manUrl = null;
            switch (opcode) {
                case IORING_OP_READ:
                    msgBuilder.append("Failed to a read from file ").append(file.path()).append(". ");
                    manUrl = "https://man7.org/linux/man-pages/man2/pwrite.2.html";
                    break;
                case IORING_OP_WRITE:
                    msgBuilder.append("Failed to a write to file ").append(file.path()).append(". ");
                    manUrl = "https://man7.org/linux/man-pages/man3/pwrite.3p.html";
                    break;
                case IORING_OP_NOP:
                    msgBuilder.append("Failed to perform a nop on file ").append(file.path()).append(". ");
                    break;
                case IORING_OP_FSYNC:
                    if (rwFlags == IORING_FSYNC_DATASYNC) {
                        msgBuilder.append("Failed to perform a fdatasync on file ");
                    } else {
                        msgBuilder.append("Failed to perform a fsync on file ");
                    }
                    msgBuilder.append(file.path()).append(". ");
                    manUrl = "https://man7.org/linux/man-pages/man2/fsync.2.html";
                    break;
                case IORING_OP_OPENAT:
                    buf.release();
                    msgBuilder.append("Failed to open file ").append(file.path()).append(". ");
                    manUrl = "https://man7.org/linux/man-pages/man2/open.2.html";
                    break;
                case IORING_OP_CLOSE:
                    msgBuilder.append("Failed to close file ").append(file.path()).append(". ");
                    manUrl = "https://man7.org/linux/man-pages/man2/close.2.html";
                    break;
                case IORING_OP_FALLOCATE:
                    msgBuilder.append("Failed to fallocate on file ").append(file.path()).append(". ");
                    manUrl = "https://man7.org/linux/man-pages/man2/fallocate.2.html";
                    break;
                default:
                    throw new IllegalStateException("Unknown opcode: " + opcode);
            }

            msgBuilder.append("Error-message: '").append(Linux.strerror(-res)).append("'.")
                    .append("Error-code: ").append(Linux.errorcode(-res)).append(". ")
                    .append(this);
            if (manUrl != null) {
                msgBuilder.append(" See ").append(manUrl).append(" for more details. ");
            }

            promise.completeWithIOException(msgBuilder.toString(), null);
        }

        private void release() {
            addr = 0;
            rwFlags = 0;
            flags = 0;
            opcode = 0;
            len = 0;
            offset = 0;
            buf = null;
            file = null;
            promise = null;
            bioAllocator.free(this);
        }

        @Override
        public String toString() {
            return "BlockIO{"
                    + "file=" + file
                    + ", offset=" + offset
                    + ", length=" + len
                    + ", opcode=" + opcodeToString(opcode)
                    + ", flags=" + flags
                    + ", rwFlags=" + rwFlags
                    + ", addr=" + addr
                    + ", buf=" + (buf == null ? "null" : buf.toDebugString())
                    + "}";
        }
    }
}
