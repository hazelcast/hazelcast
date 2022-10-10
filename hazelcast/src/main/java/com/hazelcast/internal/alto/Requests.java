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

import org.jctools.util.PaddedAtomicLong;


import static java.lang.System.currentTimeMillis;

/**
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
public class Requests {

    //final NonBlockingHashMapLong<RequestFuture> map = new NonBlockingHashMapLong<>();
    //final ConcurrentHashMap<Long,RequestFuture> map = new ConcurrentHashMap<Long,RequestFuture>();
    public final RequestSlots slots = new RequestSlots(2048);
    final PaddedAtomicLong started = new PaddedAtomicLong();
    final PaddedAtomicLong completed = new PaddedAtomicLong();
    private final int concurrentRequestLimit;
    private final long nextCallIdTimeoutMs;

    public Requests(int concurrentRequestLimit, long nextCallIdTimeoutMs) {
        this.concurrentRequestLimit = concurrentRequestLimit;
        this.nextCallIdTimeoutMs = nextCallIdTimeoutMs;
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
