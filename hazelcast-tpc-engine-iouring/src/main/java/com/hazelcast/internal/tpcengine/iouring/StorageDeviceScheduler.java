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
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.util.CircularQueue;
import com.hazelcast.internal.tpcengine.util.LongObjectHashMap;
import com.hazelcast.internal.tpcengine.util.SlabAllocator;
import com.hazelcast.internal.tpcengine.util.UnsafeLocator;
import sun.misc.Unsafe;

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

public class StorageDeviceScheduler {

    private static final Unsafe UNSAFE = UnsafeLocator.UNSAFE;

    final String path;
    IOUringEventloop eventloop;
    private final int maxConcurrent;
    private final LongObjectHashMap<CompletionHandler> handlers;
    private final SubmissionQueue sq;
    private final StorageDevice dev;
    private int concurrent;
    // a queue of IoOps that could not be submitted to io_uring because either
    // io_uring was full or because the max number of concurrent IoOps for that
    // device was exceeded.
    private final CircularQueue<IO> waitQueue;
    private final SlabAllocator<IO> ioAllocator;
    private long count;

    public StorageDeviceScheduler(StorageDevice dev, IOUringEventloop eventloop) {
        this.dev = dev;
        this.path = dev.path();
        this.maxConcurrent = dev.maxConcurrent();
        this.waitQueue = new CircularQueue<>(dev.maxWaiting());
        this.ioAllocator = new SlabAllocator<>(dev.maxWaiting(), IO::new);
        this.eventloop = eventloop;
        this.handlers = eventloop.handlers;
        this.sq = eventloop.sq;
    }

    public IO allocateIO() {
        return ioAllocator.allocate();
    }

    public Promise<Integer> submit(IO io) {

        Promise<Integer> promise = eventloop.promiseAllocator.allocate();
        io.promise = promise;

        if (concurrent < maxConcurrent) {
            offerIO(io);
        } else if (!waitQueue.offer(io)) {
            io.release();
            // todo: better approach needed
            promise.completeWithIOException(
                    "Overload. Max concurrent operations " + maxConcurrent + " dev: [" + dev.path() + "]", null);
        }

        // System.out.println("Request scheduled");

        return promise;
    }

    private void scheduleNext() {
        if (concurrent < maxConcurrent) {
            IO io = waitQueue.poll();
            if (io != null) {
                offerIO(io);
            }
        }
    }

    private void offerIO(IO io) {
        long userdata = eventloop.nextTmpHandlerId();
        handlers.put(userdata, io);

        concurrent++;

        int index = sq.nextIndex();
        if (index >= 0) {
            //System.out.println("writeSqe:" + index);
            long sqeAddr = sq.sqesAddr + index * SIZEOF_SQE;
            UNSAFE.putByte(sqeAddr + OFFSET_SQE_opcode, io.opcode);
            UNSAFE.putByte(sqeAddr + OFFSET_SQE_flags, (byte) io.flags);
            UNSAFE.putShort(sqeAddr + OFFSET_SQE_ioprio, (short) 0);
            UNSAFE.putInt(sqeAddr + OFFSET_SQE_fd, io.file.fd);
            UNSAFE.putLong(sqeAddr + OFFSET_SQE_off, io.offset);
            UNSAFE.putLong(sqeAddr + OFFSET_SQE_addr, io.addr);
            UNSAFE.putInt(sqeAddr + OFFSET_SQE_len, io.len);
            UNSAFE.putInt(sqeAddr + OFFSET_SQE_rw_flags, io.rwFlags);
            UNSAFE.putLong(sqeAddr + OFFSET_SQE_user_data, userdata);
            //System.out.println("SubmissionQueue: userdata:" + userData + " index:" + index + " io:" + opcode);
        } else {
            // todo: we need to find better solution
            throw new RuntimeException("Could not offer io: " + io);
        }
    }

    public class IO implements CompletionHandler {
        public IOBuffer buf;
        public long writeId;
        public IOUringAsyncFile file;
        public long offset;
        public int len;
        public byte opcode;
        public int flags;
        public int rwFlags;
        public long addr;
        public Promise<Integer> promise;

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
                // todo: nasty hack
                if (buf != null) {
//                    System.out.println("res:"+res);
//                    System.out.println(BufferUtil.toDebugString("buf",buf.byteBuffer()));
                    buf.incPosition(res);
                }
                // todo: this is where we want to deal with the sync.
                // so if the call is a write and we see there is a trailing fsync, check all all prior writes
                // have completed. If they have, we can issue the fsync.

                //todo: need to ensure that we don't create objects here.
                promise.complete(res);
            }

            scheduleNext();
            release();
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
            ioAllocator.free(this);
        }

        @Override
        public String toString() {
            return "IO{"
                    + "file=" + file
                    + ", offset=" + offset
                    + ", length=" + len
                    + ", opcode=" + opcodeToString(opcode)
                    + ", flags=" + flags
                    + ", rwFlags=" + rwFlags
                    + ", addr=" + addr
                    + ", buf=" + (buf == null ? "null" : buf.toDebugString())
                    + '}';
        }
    }
}
