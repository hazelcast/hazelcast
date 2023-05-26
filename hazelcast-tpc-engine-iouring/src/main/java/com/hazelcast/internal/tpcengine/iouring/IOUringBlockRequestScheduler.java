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
import com.hazelcast.internal.tpcengine.file.BlockRequest;
import com.hazelcast.internal.tpcengine.file.BlockRequestScheduler;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.util.CircularQueue;
import com.hazelcast.internal.tpcengine.util.LongObjectHashMap;
import com.hazelcast.internal.tpcengine.util.SlabAllocator;
import com.hazelcast.internal.tpcengine.util.UnsafeLocator;
import sun.misc.Unsafe;

import java.nio.charset.StandardCharsets;

import static com.hazelcast.internal.tpcengine.file.BlockRequest.BLK_REQ_OP_CLOSE;
import static com.hazelcast.internal.tpcengine.file.BlockRequest.BLK_REQ_OP_FALLOCATE;
import static com.hazelcast.internal.tpcengine.file.BlockRequest.BLK_REQ_OP_FDATASYNC;
import static com.hazelcast.internal.tpcengine.file.BlockRequest.BLK_REQ_OP_FSYNC;
import static com.hazelcast.internal.tpcengine.file.BlockRequest.BLK_REQ_OP_NOP;
import static com.hazelcast.internal.tpcengine.file.BlockRequest.BLK_REQ_OP_OPEN;
import static com.hazelcast.internal.tpcengine.file.BlockRequest.BLK_REQ_OP_READ;
import static com.hazelcast.internal.tpcengine.file.BlockRequest.BLK_REQ_OP_WRITE;
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
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_CHAR;

/**
 * Todo: The IOScheduler should be a scheduler for all storage devices. Currently it is only for a single device.
 */
public class IOUringBlockRequestScheduler implements BlockRequestScheduler {

    private static final Unsafe UNSAFE = UnsafeLocator.UNSAFE;

    final String path;
    IOUringEventloop eventloop;
    private final int concurrentLimit;
    private final LongObjectHashMap<CompletionHandler> handlers;
    private final SubmissionQueue sq;
    private final BlockDevice dev;
    private int concurrent;
    // a queue of IoOps that could not be submitted to io_uring because either
    // io_uring was full or because the max number of concurrent IoOps for that
    // device was exceeded.
    private final CircularQueue<IOUringBlockRequest> waitQueue;
    private final SlabAllocator<IOUringBlockRequest> requestAllocator;
    private long count;

    // To prevent intermediate string litter for every IOException the msgBuilder is recycled.
    private final StringBuilder msgBuilder = new StringBuilder();

    public IOUringBlockRequestScheduler(BlockDevice dev, IOUringEventloop eventloop) {
        this.dev = dev;
        this.path = dev.path();
        this.concurrentLimit = dev.concurrentLimit();
        this.waitQueue = new CircularQueue<>(dev.maxWaiting());
        this.requestAllocator = new SlabAllocator<>(dev.maxWaiting(), IOUringBlockRequest::new);
        this.eventloop = eventloop;
        this.handlers = eventloop.handlers;
        this.sq = eventloop.sq;
    }

    @Override
    public BlockRequest reserve() {
        //todo: not the right condition
        if (concurrent >= concurrentLimit) {
            return null;
        }

        return requestAllocator.allocate();
    }

    @Override
    public void submit(BlockRequest req) {
        if (concurrent < concurrentLimit) {
            offer((IOUringBlockRequest) req);
        } else if (!waitQueue.offer((IOUringBlockRequest) req)) {
            throw new IllegalStateException("Too many concurrent IOs");
        }
    }

    private void scheduleNext() {
        if (concurrent < concurrentLimit) {
            IOUringBlockRequest req = waitQueue.poll();
            if (req != null) {
                offer(req);
            }
        }
    }

    private void offer(IOUringBlockRequest req) {
        long userdata = eventloop.nextTmpHandlerId();
        handlers.put(userdata, req);

        concurrent++;

        int index = sq.nextIndex();
        if (index >= 0) {
            long sqeAddr = sq.sqesAddr + index * SIZEOF_SQE;

            switch (req.opcode) {
                case BLK_REQ_OP_NOP:
                    UNSAFE.putByte(sqeAddr + OFFSET_SQE_opcode, IORING_OP_NOP);
                    UNSAFE.putByte(sqeAddr + OFFSET_SQE_flags, (byte) 0);
                    UNSAFE.putShort(sqeAddr + OFFSET_SQE_ioprio, (short) 0);
                    UNSAFE.putInt(sqeAddr + OFFSET_SQE_fd, 0);
                    UNSAFE.putLong(sqeAddr + OFFSET_SQE_off, 0);
                    UNSAFE.putLong(sqeAddr + OFFSET_SQE_addr, 0);
                    UNSAFE.putInt(sqeAddr + OFFSET_SQE_len, 0);
                    UNSAFE.putInt(sqeAddr + OFFSET_SQE_rw_flags, 0);
                    UNSAFE.putLong(sqeAddr + OFFSET_SQE_user_data, userdata);
                    break;
                case BLK_REQ_OP_READ:
                    UNSAFE.putByte(sqeAddr + OFFSET_SQE_opcode, IORING_OP_READ);
                    UNSAFE.putByte(sqeAddr + OFFSET_SQE_flags, (byte) 0);
                    UNSAFE.putShort(sqeAddr + OFFSET_SQE_ioprio, (short) 0);
                    UNSAFE.putInt(sqeAddr + OFFSET_SQE_fd, req.file.fd);
                    UNSAFE.putLong(sqeAddr + OFFSET_SQE_off, req.offset);
                    UNSAFE.putLong(sqeAddr + OFFSET_SQE_addr, req.buf.address());
                    UNSAFE.putInt(sqeAddr + OFFSET_SQE_len, req.length);
                    UNSAFE.putInt(sqeAddr + OFFSET_SQE_rw_flags, 0);
                    UNSAFE.putLong(sqeAddr + OFFSET_SQE_user_data, userdata);
                    break;
                case BLK_REQ_OP_WRITE:
                    UNSAFE.putByte(sqeAddr + OFFSET_SQE_opcode, IORING_OP_WRITE);
                    UNSAFE.putByte(sqeAddr + OFFSET_SQE_flags, (byte) 0);
                    UNSAFE.putShort(sqeAddr + OFFSET_SQE_ioprio, (short) 0);
                    UNSAFE.putInt(sqeAddr + OFFSET_SQE_fd, req.file.fd);
                    UNSAFE.putLong(sqeAddr + OFFSET_SQE_off, req.offset);
                    UNSAFE.putLong(sqeAddr + OFFSET_SQE_addr, req.buf.address());
                    UNSAFE.putInt(sqeAddr + OFFSET_SQE_len, req.length);
                    UNSAFE.putInt(sqeAddr + OFFSET_SQE_rw_flags, 0);
                    UNSAFE.putLong(sqeAddr + OFFSET_SQE_user_data, userdata);
                    break;
                case BLK_REQ_OP_FSYNC:
                    UNSAFE.putByte(sqeAddr + OFFSET_SQE_opcode, IORING_OP_FSYNC);
                    break;
                case BLK_REQ_OP_FDATASYNC:
                    // todo: //            request.opcode = IORING_OP_FSYNC;
                    ////            // The IOURING_FSYNC_DATASYNC maps to the same position as the rw-flags
                    ////            request.rwFlags = IORING_FSYNC_DATASYNC;
                    UNSAFE.putByte(sqeAddr + OFFSET_SQE_opcode, IORING_OP_FSYNC);
                    break;
                case BLK_REQ_OP_OPEN:
                    req.buf = pathBuffer();
                    UNSAFE.putByte(sqeAddr + OFFSET_SQE_opcode, IORING_OP_OPENAT);
                    UNSAFE.putByte(sqeAddr + OFFSET_SQE_flags, (byte) 0);
                    UNSAFE.putShort(sqeAddr + OFFSET_SQE_ioprio, (short) 0);
                    UNSAFE.putInt(sqeAddr + OFFSET_SQE_fd, 0);
                    UNSAFE.putLong(sqeAddr + OFFSET_SQE_off, 0);
                    UNSAFE.putLong(sqeAddr + OFFSET_SQE_addr, req.buf.address());
                    // at the position of the length field, the permissions are stored.
                    UNSAFE.putInt(sqeAddr + OFFSET_SQE_len, req.permissions);

                    UNSAFE.putInt(sqeAddr + OFFSET_SQE_rw_flags, req.flags);
                    UNSAFE.putLong(sqeAddr + OFFSET_SQE_user_data, userdata);
                    break;
                case BLK_REQ_OP_CLOSE:
                    UNSAFE.putByte(sqeAddr + OFFSET_SQE_opcode, IORING_OP_CLOSE);
                    UNSAFE.putByte(sqeAddr + OFFSET_SQE_flags, (byte) 0);
                    UNSAFE.putShort(sqeAddr + OFFSET_SQE_ioprio, (short) 0);
                    UNSAFE.putInt(sqeAddr + OFFSET_SQE_fd, req.file.fd);
                    UNSAFE.putLong(sqeAddr + OFFSET_SQE_off, 0);
                    UNSAFE.putLong(sqeAddr + OFFSET_SQE_addr, 0);
                    UNSAFE.putInt(sqeAddr + OFFSET_SQE_len, 0);
                    UNSAFE.putInt(sqeAddr + OFFSET_SQE_rw_flags, 0);
                    UNSAFE.putLong(sqeAddr + OFFSET_SQE_user_data, userdata);
                    break;
                case BLK_REQ_OP_FALLOCATE:
                    UNSAFE.putByte(sqeAddr + OFFSET_SQE_opcode, IORING_OP_FALLOCATE);
                    break;
                default:
                    throw new IllegalStateException("Unknown request type: " + req.opcode);
            }

//            UNSAFE.putByte(sqeAddr + OFFSET_SQE_flags, (byte) req.flags);
//            UNSAFE.putShort(sqeAddr + OFFSET_SQE_ioprio, (short) 0);
//            UNSAFE.putInt(sqeAddr + OFFSET_SQE_fd, req.file.fd);
//            UNSAFE.putLong(sqeAddr + OFFSET_SQE_off, req.offset);
//            UNSAFE.putLong(sqeAddr + OFFSET_SQE_addr, req.addr);
//            UNSAFE.putInt(sqeAddr + OFFSET_SQE_len, req.len);
//            UNSAFE.putInt(sqeAddr + OFFSET_SQE_rw_flags, req.rwFlags);
//            UNSAFE.putLong(sqeAddr + OFFSET_SQE_user_data, userdata);
            //System.out.println("SubmissionQueue: userdata:" + userData + " index:" + index + " req:" + opcode);
        } else {
            // can't happen
            // todo: we need to find better solution
            throw new RuntimeException("Could not offer req: " + req);
        }
    }

    private IOBuffer pathBuffer() {
        // todo: unwanted litter.
        byte[] chars = path.getBytes(StandardCharsets.UTF_8);

        // todo: we do not need a blockIOBuffer for this; this is just used for passing the name
        // of the file as a string to the kernel.
        IOBuffer pathBuffer = eventloop.blockIOBufferAllocator().allocate(chars.length + SIZEOF_CHAR);
        pathBuffer.writeBytes(chars);
        // C strings end with \0
        pathBuffer.writeChar('\0');
        pathBuffer.flip();
        return pathBuffer;
    }

    /**
     * Represents a single I/O req on a block device. For example a read or write to disk.
     */
    class IOUringBlockRequest extends BlockRequest implements CompletionHandler {

        @Override
        public void handle(int res, int flags, long userdata) {
            concurrent--;
            if (res >= 0) {
                AsyncFileMetrics metrics = file.metrics();
                switch (opcode) {
                    case BLK_REQ_OP_NOP:
                        metrics.incNops();
                        break;
                    case BLK_REQ_OP_READ:
                        buf.incPosition(res);
                        metrics.incReads();
                        metrics.incBytesRead(res);
                        break;
                    case BLK_REQ_OP_WRITE:
                        buf.incPosition(res);
                        metrics.incWrites();
                        metrics.incBytesWritten(res);
                        break;
                    case BLK_REQ_OP_FSYNC:
                        metrics.incFsyncs();
                        break;
                    case BLK_REQ_OP_FDATASYNC:
                        metrics.incFdatasyncs();
                        break;
                    case BLK_REQ_OP_OPEN:
                        buf.release();
                        file.fd = res;
                        break;
                    case BLK_REQ_OP_CLOSE:
                        file.fd = -1;
                        break;
                    case BLK_REQ_OP_FALLOCATE:
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
                case BLK_REQ_OP_NOP:
                    msgBuilder.append("Failed to perform a nop on file ").append(file.path()).append(". ");
                    break;
                case BLK_REQ_OP_READ:
                    msgBuilder.append("Failed to a read from file ").append(file.path()).append(". ");
                    manUrl = "https://man7.org/linux/man-pages/man2/pwrite.2.html";
                    break;
                case BLK_REQ_OP_WRITE:
                    msgBuilder.append("Failed to a write to file ").append(file.path()).append(". ");
                    manUrl = "https://man7.org/linux/man-pages/man3/pwrite.3p.html";
                    break;
                case BLK_REQ_OP_FSYNC:
                    msgBuilder.append("Failed to perform a fsync on file ").append(file.path()).append(". ");
                    manUrl = "https://man7.org/linux/man-pages/man2/fsync.2.html";
                    break;
                case BLK_REQ_OP_FDATASYNC:
                    msgBuilder.append("Failed to perform a fdatasync on file ").append(file.path()).append(". ");
                    manUrl = "https://man7.org/linux/man-pages/man2/fsync.2.html";
                    break;
                case BLK_REQ_OP_OPEN:
                    buf.release();
                    msgBuilder.append("Failed to open file ").append(file.path()).append(". ");
                    manUrl = "https://man7.org/linux/man-pages/man2/open.2.html";
                    break;
                case BLK_REQ_OP_CLOSE:
                    msgBuilder.append("Failed to close file ").append(file.path()).append(". ");
                    manUrl = "https://man7.org/linux/man-pages/man2/close.2.html";
                    break;
                case BLK_REQ_OP_FALLOCATE:
                    msgBuilder.append("Failed to fallocate on file ").append(file.path()).append(". ");
                    manUrl = "https://man7.org/linux/man-pages/man2/fallocate.2.html";
                    break;
                default:
                    throw new IllegalStateException("Unknown opcode: " + opcode);
            }

            msgBuilder.append("Error-message '").append(Linux.strerror(-res)).append("' ")
                    .append("Error-code ").append(Linux.errorcode(-res)).append(". ")
                    .append(this);
            if (manUrl != null) {
                msgBuilder.append(" See ").append(manUrl).append(" for more details. ");
            }

            promise.completeWithIOException(msgBuilder.toString(), null);
        }

        private void release() {
            rwFlags = 0;
            flags = 0;
            opcode = 0;
            length = 0;
            offset = 0;
            permissions = 0;
            buf = null;
            file = null;
            promise = null;

            requestAllocator.free(this);
        }

        @Override
        public String toString() {
            return "BlockIO{"
                    + "file=" + file
                    + ", offset=" + offset
                    + ", length=" + length
                    + ", opcode=" + opcodeToString(opcode)
                    + ", flags=" + flags
                    + ", permissions=" + permissions
                    + ", rwFlags=" + rwFlags
                    + ", buf=" + (buf == null ? "null" : buf.toDebugString())
                    + "}";
        }
    }
}
