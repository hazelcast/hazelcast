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

package com.hazelcast.internal.tpc.server;

import com.hazelcast.internal.tpc.FrameCodec;
import com.hazelcast.internal.tpc.RequestFuture;
import org.jctools.util.PaddedAtomicLong;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.hazelcast.internal.util.QuickMath.nextPowerOfTwo;
import static java.lang.System.currentTimeMillis;

/**
 * The pending requests for some Reactor. This could be a local or a remote reactor.
 * <p>
 * Idea:
 * Instead of using a ConcurrentHashMap where we are going to get litter when an
 * request is inserted and when a request is removed, use an array of slots. In this
 * array you can write with a volatile write.
 * <p>
 * Then there is a second array containing the available positions. This array has a
 * head and a tail. Items are removed from the head and added to the tail. A cas is
 * needed for each of these operations.
 * <p>
 * This will resolve the litter problem. Also there is less contention on the counter;
 * although not that much of an issue unless many calls on single partition.
 * <p>
 * The problem is when requests are lost. Then the slots won't be reclaimed.
 */
@SuppressWarnings("checkstyle:MagicNumber")
public class Requests {

    final PaddedAtomicLong started = new PaddedAtomicLong();
    final PaddedAtomicLong completed = new PaddedAtomicLong();

    private final int concurrentRequestLimit;
    private final long nextCallIdTimeoutMs;
    private final AtomicReferenceArray<RequestFuture> array;
    // Currently initialized as 500 to make it easier to spot marshalling problems
    private final AtomicLong callIdGenerator = new AtomicLong(500);
    private final int mask;
    private final int capacity;

    public Requests(int concurrentRequestLimit, long nextCallIdTimeoutMs) {
        this.concurrentRequestLimit = concurrentRequestLimit;
        this.nextCallIdTimeoutMs = nextCallIdTimeoutMs;
        this.capacity = nextPowerOfTwo(concurrentRequestLimit);
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
    public RequestFuture remove(long callId) {
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
            f.request.putLong(FrameCodec.OFFSET_REQ_CALL_ID, callId);
            int index = (int) (callId & mask);
            if (array.compareAndSet(index, null, f)) {
                return callId;
            }
        }
    }

    void complete() {
        if (concurrentRequestLimit > -1) {
            completed.incrementAndGet();
        }
    }

    public long nextCallId() {
        if (concurrentRequestLimit == -1) {
            return started.incrementAndGet();
        } else {
            long endTime = currentTimeMillis() + nextCallIdTimeoutMs;
            do {
                if (completed.get() + concurrentRequestLimit > started.get()) {
                    return started.incrementAndGet();
                } else {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        throw new RuntimeException();
                    }
                }
            } while (currentTimeMillis() < endTime);

            throw new RuntimeException("Member is overloaded with requests");
        }
    }

    long nextCallId(int count) {
        if (concurrentRequestLimit == -1) {
            return started.addAndGet(count);
        } else {
            long endTime = currentTimeMillis() + nextCallIdTimeoutMs;
            do {
                if (completed.get() + concurrentRequestLimit > started.get() + count) {
                    return started.addAndGet(count);
                } else {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        throw new RuntimeException();
                    }
                }
            } while (currentTimeMillis() < endTime);

            throw new RuntimeException("Member is overloaded with requests");
        }
    }

    void shutdown() {
//        for (RequestFuture future : map.values()) {
//            future.completeExceptionally(new RuntimeException("Shutting down"));
//        }
    }
}
