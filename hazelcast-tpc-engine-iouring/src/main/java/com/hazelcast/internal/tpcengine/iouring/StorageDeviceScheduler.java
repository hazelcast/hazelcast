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
import com.hazelcast.internal.tpcengine.file.StorageDevice;
import com.hazelcast.internal.tpcengine.util.CircularQueue;
import com.hazelcast.internal.tpcengine.util.LongObjectHashMap;
import com.hazelcast.internal.tpcengine.util.SlabAllocator;

import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_FSYNC;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_READ;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_WRITE;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.opcodeToString;

public class StorageDeviceScheduler {
    private final int maxConcurrent;
    final String path;
    private final LongObjectHashMap<IOCompletionHandler> handlers;
    private final SubmissionQueue sq;
    private final StorageDevice dev;
    private int concurrent;
    // a queue of IoOps that could not be submitted to io_uring because either
    // io_uring was full or because the max number of concurrent IoOps for that
    // device was exceeded.
    private final CircularQueue<IoOp> waitQueue;
    private final SlabAllocator<IoOp> opAllocator;
    private long count;

    IOUringEventloop eventloop;

    public StorageDeviceScheduler(StorageDevice dev, IOUringEventloop eventloop) {
        this.dev = dev;
        this.path = dev.path();
        this.maxConcurrent = dev.maxConcurrent();
        this.waitQueue = new CircularQueue<>(dev.maxWaiting());
        this.opAllocator = new SlabAllocator<>(dev.maxWaiting(), IoOp::new);
        this.eventloop = eventloop;
        this.handlers = eventloop.handlers;
        this.sq = eventloop.sq;
    }

    public Promise<Integer> submit(IOUringAsyncFile file,
                                   byte opcode,
                                   int flags,
                                   int rwFlags,
                                   long bufferAddress,
                                   int length,
                                   long offset) {
        IoOp op = opAllocator.allocate();

        if (opcode == IORING_OP_FSYNC) {
            // if there are no pending writes, the fsync can immediately be used.
            // if there are pending writes, the fsync need to wait till all the writes
            // before the fsync have completed.
        } else if (opcode == IORING_OP_WRITE) {
            op.writeId = ++file.newestWriteSeq;
        } else {
            op.writeId = -1;
        }

        op.file = file;
        op.opcode = opcode;
        op.flags = flags;
        op.rwFlags = rwFlags;
        op.bufferAddress = bufferAddress;
        op.length = length;
        op.offset = offset;

//        count++;
//        if (count % 100000 == 0) {
//            System.out.println("count: " + count + " waitQueue.size:" + waitQueue.size() + " qd:" + concurrent);
//        }

        Promise<Integer> promise = eventloop.promiseAllocator.allocate();
        op.promise = promise;

        if (concurrent < maxConcurrent) {
            offerOp(op);
        } else if (!waitQueue.offer(op)) {
            opAllocator.free(op);
            // todo: better approach needed
            promise.completeWithIOException(
                    "Overload. Max concurrent operations " + maxConcurrent + " dev: [" + dev.path() + "]", null);
        }

        // System.out.println("Request scheduled");

        return promise;
    }

    private void scheduleNext() {
        if (concurrent < maxConcurrent) {
            IoOp op = waitQueue.poll();
            if (op != null) {
                offerOp(op);
            }
        }
    }

    private void offerOp(IoOp op) {
        long userdata = eventloop.nextTmpHandlerId();
        handlers.put(userdata, op);

        concurrent++;

        boolean offered = sq.offer(
                op.opcode,
                op.flags,
                op.rwFlags,
                op.file.fd,
                op.bufferAddress,
                op.length,
                op.offset,
                userdata);

        // todo: we need to find better solution
        if (!offered) {
            throw new RuntimeException("Could not offer op: " + op);
        }
    }

    private class IoOp implements IOCompletionHandler {
        private long writeId;
        private IOUringAsyncFile file;
        private long offset;
        private int length;
        private byte opcode;
        private int flags;
        private int rwFlags;
        private long bufferAddress;
        private Promise<Integer> promise;

        @Override
        public void handle(int res, int flags, long userdata) {
            concurrent--;
            if (res < 0) {
                String message = "I/O error"
                        + ", error-message: [" + Linux.strerror(-res) + "]"
                        + ", error-code: " + Linux.errorcode(-res)
                        + ", path: [" + file.path() + "]"
                        + ", " + this;
                promise.completeWithIOException(message, null);

                // todo: if there is trailing fsync

            } else {
                // todo: this is where we want to deal with the sync.
                // so if the call is a write and we see there is a trailing fsync, check all all prior writes
                // have completed. If they have, we can issue the fsync.

                //todo: need to ensure that we don't create objects here.
                promise.complete(res);
            }

            scheduleNext();
            file = null;
            promise = null;
            opAllocator.free(this);
        }

        @Override
        public String toString() {
            return "Op{" +
                    "file=" + file +
                    ", offset=" + offset +
                    ", length=" + length +
                    ", opcode=" + opcodeToString(opcode) +
                    ", flags=" + flags +
                    ", rwFlags=" + rwFlags +
                    ", bufferAddress=" + bufferAddress +
                    '}';
        }
    }
}
