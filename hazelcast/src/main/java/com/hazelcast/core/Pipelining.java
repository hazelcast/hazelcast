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

package com.hazelcast.core;


import com.hazelcast.map.IMap;
import com.hazelcast.spi.annotation.Beta;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;


/**
 * The Pipelining can be used to speed up requests. It is build on top of asynchronous
 * requests like e.g. {@link IMap#getAsync(Object)} or any other asynchronous call.
 *
 * The main purpose of the Pipelining is to control the number of concurrent requests
 * when using asynchronous invocations. This can be done by setting the depth using
 * the constructor. So you could set the depth to e.g 100 and do 1000 calls. That means
 * that at any given moment, there will only be 100 concurrent requests.
 *
 * It depends on the situation what the optimal depth (number of invocations in
 * flight) should be. If it is too high, you can run into memory related problems.
 * If it is too low, it will provide little or no performance advantage at all. In
 * most cases a Pipelining and a few hundred map/cache puts/gets should not lead to any
 * problems. For testing purposes we frequently have a Pipelining of 1000 or more
 * concurrent requests to be able to saturate the system.
 *
 * The Pipelining can't be used for transaction purposes. So you can't create a
 * Pipelining, add a set of asynchronous request and then not call {@link #results()}
 * to prevent executing these requests. Invocations can be executed before the
 * {@link #results()} is called.
 *
 * Pipelining can be used by both clients or members.
 *
 * The Pipelining isn't threadsafe. So only a single thread should add requests to
 * the Pipelining and wait for results.
 *
 * Currently all {@link CompletionStage} and their responses are stored in the
 * Pipelining. So be careful executing a huge number of request with a single Pipelining
 * because it can lead to a huge memory bubble. In this cases it is better to
 * periodically, after waiting for completion, to replace the Pipelining by a new one.
 * In the future we might provide this as an out of the box experience, but currently
 * we do not.
 *
 * A Pipelining provides its own backpressure on the system. So there will not be more
 * in flight invocations than the depth of the Pipelining. This means that the Pipelining
 * will work fine when backpressure on the client/member is disabled (default). Also
 * when it is enabled it will work fine, but keep in mind that the number of concurrent
 * invocations in the Pipelining could be lower than the configured number of invocation
 * of the Pipelining because the backpressure on the client/member is leading.
 *
 * The Pipelining has been marked as Beta since we need to see how the API needs to
 * evolve. But there is no problem using it in production. We use similar techniques
 * to achieve high performance.
 *
 * @param <E> the result type of the Pipelining
 */
@Beta
public class Pipelining<E> {

    private final AtomicInteger permits = new AtomicInteger();
    private final List<CompletionStage<E>> futures = new ArrayList<>();
    private Thread thread;

    /**
     * Creates a Pipelining with the given depth.
     *
     * @param depth the maximum number of concurrent calls allowed in this Pipelining.
     * @throws IllegalArgumentException if depth smaller than 1. But if you use depth 1, it means that
     *                                  every call is sync and you will not benefit from pipelining at all.
     */
    public Pipelining(int depth) {
        checkPositive(depth, "depth must be positive");
        this.permits.set(depth);
    }

    /**
     * Returns the results.
     * <p>
     * The results are returned in the order the requests were done.
     * <p>
     * This call waits till all requests have completed.
     *
     * @return the List of results.
     * @throws Exception is something fails getting the results.
     */
    public List<E> results() throws Exception {
        List<E> result = new ArrayList<>(futures.size());
        for (CompletionStage<E> f : futures) {
            result.add(f.toCompletableFuture().get());
        }
        return result;
    }

    /**
     * Adds a future to this Pipelining or blocks until there is capacity to add the future to the Pipelining.
     * <p>
     * This call blocks until there is space in the Pipelining, but it doesn't mean that the invocation that
     * returned the CompletionStage got blocked.
     *
     * @param future the future to add.
     * @return the future added.
     * @throws InterruptedException if the Thread got interrupted while adding the request to the Pipelining.
     * @throws NullPointerException if future is null.
     */
    public CompletionStage<E> add(CompletionStage<E> future) throws InterruptedException {
        checkNotNull(future, "future can't be null");
        this.thread = Thread.currentThread();

        down();
        futures.add(future);
        future.whenCompleteAsync((response, t) -> up(), CALLER_RUNS);
        return future;
    }

    private void down() throws InterruptedException {
        for (; ; ) {
            int current = permits.get();
            int update = current - 1;
            if (!permits.compareAndSet(current, update)) {
                // we failed to cas, so lets try again.
                continue;
            }

            while (permits.get() == -1) {
                LockSupport.park();
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
            }

            return;
        }
    }

    private void up() {
        for (; ; ) {
            int current = permits.get();
            int update = current + 1;
            if (permits.compareAndSet(current, update)) {
                if (current == -1) {
                    LockSupport.unpark(thread);
                }
                return;
            }
        }
    }
}
