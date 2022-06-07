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

package com.hazelcast.tpc.engine.iouring;

import com.hazelcast.internal.util.collection.Int2ObjectHashMap;
import com.hazelcast.tpc.engine.AsyncFile;
import com.hazelcast.tpc.engine.Promise;
import com.hazelcast.tpc.engine.iouring.IOUringEventloop.IOUringUnsafe;
import com.hazelcast.tpc.util.Slots;
import com.hazelcast.tpc.util.CircularQueue;
import com.hazelcast.tpc.util.SlabAllocator;

import io.netty.channel.unix.Buffer;
import io.netty.incubator.channel.uring.IOUringSubmissionQueue;

import java.io.IOException;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;


public class IORequestScheduler {

    private final SlabAllocator<IoRequest> ioRequestAllocator;

    private IOUringEventloop eventloop;
    private IOUringSubmissionQueue sq;
    private final Int2ObjectHashMap<AsyncFileIoRequests> fileRequests = new Int2ObjectHashMap<>();
    private final int maxConcurrency;
    private final CircularQueue<IoRequest> pending;
    private IOUringUnsafe unsafe;
    private int concurrent;

    public IORequestScheduler(int maxConcurrency, int maxPending) {
        this.maxConcurrency = checkPositive("maxConcurrency", maxConcurrency);
        this.ioRequestAllocator = new SlabAllocator<>(maxPending, IoRequest::new);
        this.pending = new CircularQueue<>(maxPending);
    }

    public void init(IOUringEventloop eventloop) {
        this.eventloop = eventloop;
        this.unsafe = (IOUringUnsafe) eventloop.unsafe();
        this.sq = eventloop.sq;
    }

    // todo: we can do actual registration on the rb.
    public void register(AsyncFile file) {
        checkNotNull(file);

        System.out.println("register " + file.fd());

        AsyncFileIoRequests ioRequests = new AsyncFileIoRequests(file);
        fileRequests.put(file.fd(), ioRequests);
        eventloop.completionListeners.put(file.fd(), ioRequests);
    }

    private class AsyncFileIoRequests implements CompletionListener {
        private final Slots<IoRequest> ioSlots = new Slots<>(maxConcurrency);
        private final AsyncFile file;

        private AsyncFileIoRequests(AsyncFile file) {
            this.file = file;
        }

        @Override
        public void handle(int fd, int res, int flags, byte op, short data) {
            IoRequest ioRequest = ioSlots.remove(data);
            if (ioRequest != null) {
                if (res < 0) {
                    ioRequest.promise.completeExceptionally(
                            new IOException(file.path() + " res=" + -res + " for info see: " +
                                    "https://www.thegeekstuff.com/2010/10/linux-error-codes/"));
                } else {
                    ioRequest.promise.complete(true);
                }

                ioRequest.clear();
                ioRequestAllocator.free(ioRequest);
            }

            issueNext();
        }
    }

    private void issueNext() {
        IoRequest ioRequest = pending.poll();
        if (ioRequest == null) {
            concurrent--;
        } else {
            submit(ioRequest);
        }
    }

    public Promise schedule(byte op, int flags, int rwFlags, int fd, long bufferAddress, int length, long offset) {
        IoRequest ioRequest = ioRequestAllocator.allocate();
        ioRequest.op = op;
        ioRequest.flags = flags;
        ioRequest.rwFlags = rwFlags;
        ioRequest.fd = fd;
        ioRequest.bufferAddress = bufferAddress;
        ioRequest.length = length;
        ioRequest.offset = offset;

        Promise promise = unsafe.newPromise();
        ioRequest.promise = promise;

        ioRequest.ioRequests = fileRequests.get(ioRequest.fd);

        if (pending.isEmpty()) {
            submit(ioRequest);
        } else if (pending.offer(ioRequest)) {
            issueNext();
        } else {
            promise.completeExceptionally(new IOException("Overload"));
        }

        return promise;
    }

    private void submit(IoRequest ioRequest) {
        short ioRequestId = (short) ioRequest.ioRequests.ioSlots.insert(ioRequest);

        boolean x = sq.enqueueSqe(
                ioRequest.op,
                ioRequest.flags,
                ioRequest.rwFlags,
                ioRequest.fd,
                ioRequest.bufferAddress,
                ioRequest.length,
                ioRequest.offset,
                ioRequestId);
    }

    private static class IoRequest {
        private int fd;
        private long offset;
        private int length;
        private byte op;
        private int flags;
        private int rwFlags;
        private long bufferAddress;
        private Promise promise;
        private AsyncFileIoRequests ioRequests;

        void clear() {
            flags = 0;
            rwFlags = 0;
            length = 0;
            promise = null;
            ioRequests = null;
        }
    }
}
