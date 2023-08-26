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

import com.hazelcast.internal.tpcengine.file.AsyncFile;
import com.hazelcast.internal.tpcengine.file.StorageRequest;
import com.hazelcast.internal.tpcengine.file.StorageScheduler;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpcengine.iobuffer.NonConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpcengine.util.UnsafeLocator;
import sun.misc.Unsafe;

import java.nio.charset.StandardCharsets;

import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_CLOSE;
import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_FALLOCATE;
import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_FDATASYNC;
import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_FSYNC;
import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_NOP;
import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_OPEN;
import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_READ;
import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_WRITE;
import static com.hazelcast.internal.tpcengine.file.StorageRequest.storageReqOpcodeToString;
import static com.hazelcast.internal.tpcengine.iouring.CompletionQueue.TYPE_STORAGE;
import static com.hazelcast.internal.tpcengine.iouring.CompletionQueue.decodeIndex;
import static com.hazelcast.internal.tpcengine.iouring.CompletionQueue.decodeOpcode;
import static com.hazelcast.internal.tpcengine.iouring.CompletionQueue.encodeUserdata;
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
import static com.hazelcast.internal.tpcengine.iouring.Uring.IORING_FSYNC_DATASYNC;
import static com.hazelcast.internal.tpcengine.iouring.Uring.IORING_OP_CLOSE;
import static com.hazelcast.internal.tpcengine.iouring.Uring.IORING_OP_FALLOCATE;
import static com.hazelcast.internal.tpcengine.iouring.Uring.IORING_OP_FSYNC;
import static com.hazelcast.internal.tpcengine.iouring.Uring.IORING_OP_NOP;
import static com.hazelcast.internal.tpcengine.iouring.Uring.IORING_OP_OPENAT;
import static com.hazelcast.internal.tpcengine.iouring.Uring.IORING_OP_READ;
import static com.hazelcast.internal.tpcengine.iouring.Uring.IORING_OP_WRITE;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_CHAR;
import static com.hazelcast.internal.tpcengine.util.BitUtil.nextPowerOfTwo;
import static com.hazelcast.internal.tpcengine.util.ExceptionUtil.newUncheckedIOException;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkPositive;
import static java.lang.Math.min;


/**
 * A io_uring specific {@link StorageScheduler} that processes all requests
 * in FIFO order.
 * <p/>
 * There are 3 important queues:
 * <ol>
 *     <li>
 *          stagingQueue: this is the queue where scheduled request initially
 *          end up.
 *     </li>
 *     <li>
 *         submissionQueue: on every scheduled tick, requests from the staging
 *         queue are moved to the submission queue.
 *     </li>
 *     <li>
 *         completionQueue: once io_uring has completed the requests, the
 *         completed requests end up at the completion queue. The completionQueue
 *         is processed by the {@link UringEventloop}.
 *     </li>
 * </ol>
 */
// todo: remove magic number
@SuppressWarnings({"checkstyle:MethodLength",
        "checkstyle:MemberName",
        "checkstyle:LocalVariableName",
        "checkstyle:MagicNumber"})
public final class UringFifoStorageScheduler implements StorageScheduler, CompletionHandler {

    private static final Unsafe UNSAFE = UnsafeLocator.UNSAFE;

    // To prevent intermediate string litter for every IOException the msgBuilder is recycled.
    private final IOBufferAllocator pathAllocator;
    private final StringBuilder errorMsgBuilder = new StringBuilder();
    private final UringStorageRequest[] requests;
    private final StagingQueue stagingQueue;
    private final SubmissionQueue submissionQueue;
    private final CompletionQueue completionQueue;
    private final int submitLimit;
    private int submitCount;
    private final UringStorageRequest[] pool;
    private int poolAllocIndex;
    private final int poolCapacity;

    public UringFifoStorageScheduler(Uring uring,
                                     int submitLimit,
                                     int pendingLimit) {
        this.submitLimit = submitLimit;
        this.pathAllocator = new NonConcurrentIOBufferAllocator(512, true);
        this.stagingQueue = new StagingQueue(pendingLimit);
        this.submissionQueue = uring.submissionQueue();
        this.completionQueue = uring.completionQueue();
        this.requests = new UringStorageRequest[pendingLimit];
        this.poolCapacity = pendingLimit;
        this.pool = new UringStorageRequest[poolCapacity];
        for (int k = 0; k < requests.length; k++) {
            UringStorageRequest req = new UringStorageRequest(k);
            requests[k] = req;
            pool[k] = req;
        }
    }

    @Override
    public StorageRequest allocate() {
        if (poolAllocIndex == poolCapacity - 1) {
            return null;
        }

        UringStorageRequest req = pool[poolAllocIndex];
        // the slot in the pool doesn't need to be nulled. It saves a write
        // barrier and it won't cause a memory leak since the number of request
        // is fixed and eventually all requests will return to the pool and we
        // don't take any choices on the nullability of the slot.
        poolAllocIndex++;
        return req;
    }

    @Override
    public void schedule(StorageRequest req) {
        if (!stagingQueue.offer((UringStorageRequest) req)) {
            throw new IllegalStateException("Too many concurrent requests");
        }
    }

    @Override
    public boolean tick() {
        // prepare as many staged requests as allowed in the submission queue.
        final StagingQueue stagingQueue = this.stagingQueue;
        final int toSubmit = min(submitLimit - submitCount, stagingQueue.size());
        final UringStorageRequest[] array = stagingQueue.array;
        final int mask = stagingQueue.mask;
        for (int k = 0; k < toSubmit; k++) {
            UringStorageRequest req = array[(int) (stagingQueue.head & mask)];
            stagingQueue.head++;
            int sqIndex = submissionQueue.nextSqeIndex();
            if (sqIndex < 0) {
                throw new IllegalStateException("No space in submission queue");
            }
            this.submitCount++;

            // todo: this should be replaced by calls to the submission queue.
            long sqeAddr = submissionQueue.sqesAddr + sqIndex * SIZEOF_SQE;
            byte sqe_opcode;
            int sqe_fd = 0;
            long sqe_addr = 0;
            int sqe_rw_flags = 0;
            int sqe_len = 0;
            switch (req.opcode) {
                case STR_REQ_OP_NOP:
                    sqe_opcode = IORING_OP_NOP;
                    break;
                case STR_REQ_OP_READ:
                    sqe_opcode = IORING_OP_READ;
                    sqe_fd = req.file.fd;
                    sqe_addr = req.buffer.address();
                    sqe_len = req.length;
                    break;
                case STR_REQ_OP_WRITE:
                    sqe_opcode = IORING_OP_WRITE;
                    sqe_fd = req.file.fd;
                    sqe_addr = req.buffer.address();
                    sqe_len = req.length;
                    break;
                case STR_REQ_OP_FSYNC:
                    sqe_opcode = IORING_OP_FSYNC;
                    sqe_fd = req.file.fd;
                    break;
                case STR_REQ_OP_FDATASYNC:
                    sqe_opcode = IORING_OP_FSYNC;
                    sqe_fd = req.file.fd;
                    // todo: //            request.opcode = IORING_OP_FSYNC;
                    ////            // The IOURING_FSYNC_DATASYNC maps to the same position as the rw-flags
                    ////            request.rwFlags = IORING_FSYNC_DATASYNC;
                    sqe_rw_flags = IORING_FSYNC_DATASYNC;
                    break;
                case STR_REQ_OP_OPEN:
                    sqe_opcode = IORING_OP_OPENAT;
                    req.buffer = pathAsIOBuffer(req);
                    sqe_addr = req.buffer.address();
                    sqe_len = req.permissions;
                    sqe_rw_flags = req.flags;
                    break;
                case STR_REQ_OP_CLOSE:
                    sqe_opcode = IORING_OP_CLOSE;
                    sqe_fd = req.file.fd;
                    break;
                case STR_REQ_OP_FALLOCATE:
                    sqe_opcode = IORING_OP_FALLOCATE;
                    break;
                default:
                    throw new RuntimeException("Unknown opcode: " + req.opcode + " this=" + this);
            }

            UNSAFE.putByte(sqeAddr + OFFSET_SQE_opcode, sqe_opcode);
            UNSAFE.putInt(sqeAddr + OFFSET_SQE_fd, sqe_fd);
            UNSAFE.putByte(sqeAddr + OFFSET_SQE_flags, (byte) 0);
            UNSAFE.putShort(sqeAddr + OFFSET_SQE_ioprio, (short) 0);
            UNSAFE.putInt(sqeAddr + OFFSET_SQE_fd, sqe_fd);
            UNSAFE.putLong(sqeAddr + OFFSET_SQE_off, req.offset);
            UNSAFE.putLong(sqeAddr + OFFSET_SQE_addr, sqe_addr);
            UNSAFE.putInt(sqeAddr + OFFSET_SQE_len, sqe_len);
            UNSAFE.putInt(sqeAddr + OFFSET_SQE_rw_flags, sqe_rw_flags);
            long userdata = encodeUserdata(TYPE_STORAGE, sqe_opcode, req.index);
            UNSAFE.putLong(sqeAddr + OFFSET_SQE_user_data, userdata);
        }

        // Completion events are processed in the eventloop, so we
        // don't need to deal with that here.

        return false;
    }

    @Override
    public void complete(int res, int flags, long userdata) {
        byte opcode = decodeOpcode(userdata);
        int index = decodeIndex(userdata);

        UringStorageRequest req = requests[index];
        switch (opcode) {
            case IORING_OP_NOP:
                completeNop(req, res);
                break;
            case IORING_OP_READ:
                completeRead(req, res);
                break;
            case IORING_OP_WRITE:
                completeWrite(req, res);
                break;
            case IORING_OP_FSYNC:
                completeFSync(req, res);
                break;
            case IORING_OP_OPENAT:
                completeOpenAt(req, res);
                break;
            case IORING_OP_CLOSE:
                completeClose(req, res);
                break;
            case IORING_OP_FALLOCATE:
                completeFallocate(req, res);
                break;
            default:
                throw new IllegalStateException("Unknown opcode:" + Uring.opcodeToString(opcode));
        }

        // since we just completed a submitted call, the number can be decreased.
        submitCount--;

        // clear the request.
        clear(req);

        // return the request to the pool
        poolAllocIndex--;
        pool[poolAllocIndex] = req;
    }

    private static void clear(UringStorageRequest req) {
        req.rwFlags = 0;
        req.flags = 0;
        req.opcode = 0;
        req.length = 0;
        req.offset = 0;
        req.permissions = 0;
        req.buffer = null;
        req.file = null;
        req.callback = null;
    }

    private void completeNop(UringStorageRequest req, int res) {
        AsyncFile.Metrics metrics = req.file.metrics();
        if (res >= 0) {
            metrics.incNops();
            req.callback.accept(res, null);
        } else {
            errorMsgBuilder.append("Failed to perform a nop on file ").append(req.file.path()).append(". ");
            String msg = completeErrorMsg(res, null);
            req.callback.accept(res, newUncheckedIOException(msg, null));
        }
    }

    private void completeWrite(UringStorageRequest req, int res) {
        AsyncFile.Metrics metrics = req.file.metrics();
        if (res >= 0) {
            req.buffer.incPosition(res);
            metrics.incWrites();
            metrics.incBytesWritten(res);
            req.callback.accept(res, null);
        } else {
            errorMsgBuilder.append("Failed to a write to file ").append(req.file.path()).append(". ");
            String msg = completeErrorMsg(res, "https://man7.org/linux/man-pages/man3/pwrite.3p.html");
            req.callback.accept(res, newUncheckedIOException(msg, null));
        }
    }

    private void completeRead(UringStorageRequest req, int res) {
        AsyncFile.Metrics metrics = req.file.metrics();
        if (res >= 0) {
            req.buffer.incPosition(res);
            metrics.incReads();
            metrics.incBytesRead(res);
            req.callback.accept(res, null);
        } else {
            errorMsgBuilder.append("Failed to a read from file ").append(req.file.path()).append(". ");
            String msg = completeErrorMsg(res, "https://man7.org/linux/man-pages/man2/read.2.html");
            req.callback.accept(res, newUncheckedIOException(msg, null));
        }
    }

    private void completeFallocate(UringStorageRequest req, int res) {
        AsyncFile.Metrics metrics = req.file.metrics();
        if (res >= 0) {
            req.callback.accept(res, null);
        } else {
            errorMsgBuilder.append("Failed to fallocate on file ").append(req.file.path()).append(". ");
            String msg = completeErrorMsg(res, "https://man7.org/linux/man-pages/man2/fallocate.2.html");
            req.callback.accept(res, newUncheckedIOException(msg, null));
        }
    }

    private void completeClose(UringStorageRequest req, int res) {
        AsyncFile.Metrics metrics = req.file.metrics();
        if (res >= 0) {
            req.file.fd = -1;
            req.callback.accept(res, null);
        } else {
            errorMsgBuilder.append("Failed to close file ").append(req.file.path()).append(". ");
            String msg = completeErrorMsg(res, "https://man7.org/linux/man-pages/man2/close.2.html");
            req.callback.accept(res, newUncheckedIOException(msg, null));
        }
    }

    private void completeOpenAt(UringStorageRequest req, int res) {
        AsyncFile.Metrics metrics = req.file.metrics();
        if (res >= 0) {
            // this buffer is not passed from the outside, it is passed in the writeSqe function above.
            req.buffer.release();
            req.file.fd = res;
            req.callback.accept(res, null);
        } else {
            req.buffer.release();
            errorMsgBuilder.append("Failed to open file ").append(req.file.path()).append(". ");
            String msg = completeErrorMsg(res, "https://man7.org/linux/man-pages/man2/open.2.html");
            req.callback.accept(res, newUncheckedIOException(msg, null));
        }
    }

    private void completeFSync(UringStorageRequest req, int res) {
        AsyncFile.Metrics metrics = req.file.metrics();
        if (res >= 0) {
            metrics.incFsyncs();
            req.callback.accept(res, null);
        } else {
            errorMsgBuilder.append("Failed to perform a fsync on file ").append(req.file.path()).append(". ");
            String msg = completeErrorMsg(res, "https://man7.org/linux/man-pages/man2/fsync.2.html");
            req.callback.accept(res, newUncheckedIOException(msg, null));
        }
    }

    private void completeFDataSync(UringStorageRequest req, int res) {
        AsyncFile.Metrics metrics = req.file.metrics();
        if (res >= 0) {
            metrics.incFdatasyncs();
            req.callback.accept(res, null);
        } else {
            errorMsgBuilder.append("Failed to perform a fdatasync on file ").append(req.file.path()).append(". ");
            String msg = completeErrorMsg(res, "https://man7.org/linux/man-pages/man2/fsync.2.html");
            req.callback.accept(res, newUncheckedIOException(msg, null));
        }
    }

    private String completeErrorMsg(int res, String manUrl) {
        errorMsgBuilder.append("Error-message '").append(Linux.strerror(-res)).append("' ")
                .append("Error-code ").append(Linux.errorcode(-res)).append(". ")
                .append(this);

        if (manUrl != null) {
            errorMsgBuilder.append(" See ").append(manUrl).append(" for more details. ");
        }

        String msg = errorMsgBuilder.toString();
        errorMsgBuilder.setLength(0);
        return msg;
    }

    IOBuffer pathAsIOBuffer(UringStorageRequest req) {
        // todo: unwanted litter.
        byte[] chars = req.file.path().getBytes(StandardCharsets.UTF_8);

        IOBuffer pathBuffer = pathAllocator.allocate(chars.length + SIZEOF_CHAR);
        pathBuffer.writeBytes(chars);
        // C strings end with \0
        pathBuffer.writeChar('\0');
        pathBuffer.flip();
        return pathBuffer;
    }

    final class UringStorageRequest extends StorageRequest {

        private final int index;

        private UringStorageRequest(int index) {
            this.index = index;
        }
    }

    private static final class StagingQueue {
        private final UringStorageRequest[] array;
        private final int mask;
        private final int capacity;
        private long head;
        private long tail = -1;

        private StagingQueue(int capacity) {
            int fixedCapacity = nextPowerOfTwo(checkPositive(capacity, "capacity"));
            this.capacity = fixedCapacity;
            this.array = new UringStorageRequest[capacity];
            this.mask = fixedCapacity - 1;
        }

        private boolean offer(UringStorageRequest req) {
            if (tail - head + 1 == capacity) {
                return false;
            }

            long t = tail + 1;
            int index = (int) (t & mask);
            array[index] = req;
            tail = t;
            return true;
        }

        private int size() {
            return (int) (tail - head + 1);
        }
    }
}
