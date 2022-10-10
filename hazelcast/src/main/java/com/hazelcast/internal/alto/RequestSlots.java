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

package com.hazelcast.internal.alto;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.hazelcast.internal.util.QuickMath.nextPowerOfTwo;
import static com.hazelcast.internal.alto.FrameCodec.OFFSET_REQ_CALL_ID;

/**
 * Slots for RequestFutures. Instead of using a ConcurrentHashMap and causing litter, there is an array with
 * an increasing counter that is used to find a free slot.
 * <p>
 * TODO:
 * <p>
 * - Backpressure is needed when no free slot can be found.
 * <p>
 * - If there are no free slots, the counter will be increased
 * at a very high rate because the threads will spin on that
 * <p>
 * - Two Concurrent threads will increase the counter and then access the array.
 * So they will run into contention twice. If the callId would be reversed bitwise
 * for lookup purposes, they will still content on the counter, but not on the array.
 * <p>
 * - Perhaps not use a shared
 */
public class RequestSlots {

    private final AtomicReferenceArray<RequestFuture> array;
    private final AtomicLong callIdGenerator = new AtomicLong();
    private final int mask;

    RequestSlots(int capacity) {
        capacity = nextPowerOfTwo(capacity);
        this.mask = capacity - 1;
        this.array = new AtomicReferenceArray<>(capacity);
    }

    /**
     * Removes the RequestFuture with the given callId.
     * <p>
     * If the RequestFuture with the callId doesn't exist, null is returned.
     *
     * @param callId the callIo of the RequestFuture to look for.
     * @return the removed RequestFuture or null if the RequestFuture with the given callId isn't found.
     */
    RequestFuture remove(long callId) {
        int index = (int) (callId & mask);

        // It could be that the future we are looking for already has been removed. So we need to
        // check the callId of the found future that is mapped to the same slot.
        for (; ; ) {
            RequestFuture future = array.get(index);
            if (future == null || future.callId != callId) {
                return null;
            }

            if (array.compareAndSet(index, future, null)) {
                return future;
            }
        }
    }

    /**
     * Puts the RequestFuture. It will automatically create a callId, assign it
     * to the requestFuture and put it into the appropriate location in the request.
     *
     * @param f the RequestFuture to put.
     * @return the callId of the RequestFuture that was put.
     */
    public long put(RequestFuture f) {
        for (; ; ) {
            long callId = callIdGenerator.getAndIncrement();
            f.callId = callId;
            f.request.putLong(OFFSET_REQ_CALL_ID, callId);
            int index = (int) (callId & mask);
            if (array.compareAndSet(index, null, f)) {
                return callId;
            }
        }
    }
}
