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

import com.hazelcast.internal.tpc.Promise;
import com.hazelcast.internal.tpc.util.CircularQueue;
import com.hazelcast.internal.tpc.util.LongObjectHashMap;
import com.hazelcast.internal.tpc.util.SlabAllocator;

import java.io.IOException;

public class StorageDeviceScheduler {
    private final int maxConcurrent;
    final String path;
    private final LongObjectHashMap<IOCompletionHandler> handlers;
    private final SubmissionQueue sq;
    private int concurrent;
    private final CircularQueue<Op> pending;
    private final SlabAllocator<Op> opAllocator;
    private final IOUringEventloop eventloop;

    public StorageDeviceScheduler(String path,
                                  int maxConcurrent,
                                  int maxPending,
                                  IOUringEventloop eventloop) {
        this.path = path;
        this.maxConcurrent = maxConcurrent;
        this.pending = new CircularQueue<>(maxPending);
        this.opAllocator = new SlabAllocator<>(maxPending, Op::new);
        this.eventloop = eventloop;
        this.handlers = eventloop.handlers;
        this.sq = eventloop.sq;
    }

    public Promise<Integer> schedule(IOUringAsyncFile file,
                                     byte opcode,
                                     int flags,
                                     int rwFlags,
                                     long bufferAddress,
                                     int length,
                                     long offset) {
        Op op = opAllocator.allocate();
        op.file = file;
        op.opcode = opcode;
        op.flags = flags;
        op.rwFlags = rwFlags;
        op.bufferAddress = bufferAddress;
        op.length = length;
        op.offset = offset;

        Promise<Integer> promise = eventloop.promiseAllocator.allocate();
        op.promise = promise;

        if (concurrent < maxConcurrent) {
            offerOp(op);
        } else if (!pending.offer(op)) {
            opAllocator.free(op);
            // todo: better approach needed
            promise.completeExceptionally(new IOException("Overload "));
        }

        // System.out.println("Request scheduled");

        return promise;
    }

    private void submitNext() {
        if (concurrent < maxConcurrent) {
            Op req = pending.poll();
            if (req != null) {
                offerOp(req);
            }
        }
    }

    private void offerOp(Op op) {
        //System.out.println("Offer operation");

        long userdata = eventloop.nextTemporaryHandlerId();
        //System.out.println("userdata:"+userdata);
        handlers.put(userdata, op);

        //System.out.println("scheduled: "+userdata+" startUserData:"+startUserdata);

        concurrent++;

        // todo: we are not doing anything with the returned value.
        // todo: we need to apply some magic so that the
        boolean offered = sq.offer(
                op.opcode,
                op.flags,
                op.rwFlags,
                op.file.fd,
                op.bufferAddress,
                op.length,
                op.offset,
                userdata);

        if (!offered) {
            throw new RuntimeException("Could not offer op: " + op);
        }
    }

    private class Op implements IOCompletionHandler {
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
//            if(res>=0) {
//                System.out.println("Completed operation: " + userdata + " res:" + res);
//            }else{
//                System.out.println("Completed with failure: " + userdata + " res:" + res+" "+Linux.strerror(-res));
//            }

            concurrent--;
            if (res < 0) {
                promise.completeExceptionally(
                        new IOException(file.path() + " res=" + -res + " op=" + opcode + " " + Linux.strerror(-res)));
            } else {
                //todo: need to ensure that we don't create objects here.
                promise.complete(res);
            }

            submitNext();
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
                    ", opcode=" + opcode +
                    ", flags=" + flags +
                    ", rwFlags=" + rwFlags +
                    ", bufferAddress=" + bufferAddress +
                    ", fut=" + promise +
                    '}';
        }
    }
}
