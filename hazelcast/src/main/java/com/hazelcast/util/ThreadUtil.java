/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import java.util.concurrent.TimeoutException;

import static java.lang.String.format;

/**
 * Utility class for threads.
 */
public final class ThreadUtil {

    private static final ThreadLocal<Long> THREAD_LOCAL = new ThreadLocal<Long>();

    // we don't want any instances
    private ThreadUtil() {
    }

    /**
     * Await for threads to complete termination.
     * <p/>
     * Each thread is given the timeout. So if there are 10 threads, and the timeout is 1 second, in theory to wait
     * for termination of all the thread, one could wait 10 seconds.
     *
     * @param timeoutMs the timeout in ms. A timeout of 0 means waiting indefinitely.
     * @param threads   the threads to wait for completion.
     * @throws java.lang.InterruptedException        if the calling thread was interrupted while waiting.
     * @throws java.util.concurrent.TimeoutException if the thread has not terminated within the given timeout.
     * @throws IllegalArgumentException              if timeout smaller than 0.
     */
    public static void awaitTermination(long timeoutMs, Thread... threads) throws InterruptedException, TimeoutException {
        for (Thread thread : threads) {
            thread.join(timeoutMs);

            if (!thread.isAlive()) {
                continue;
            }

            throw new TimeoutException(format("Thread %s failed to complete within %s ms", thread.getName(), timeoutMs));
        }
    }

    /**
     * Get the thread id
     *
     * @return the thread id
     */
    public static long getThreadId() {
        final Long threadId = THREAD_LOCAL.get();
        if (threadId != null) {
            return threadId;
        }
        return Thread.currentThread().getId();
    }

    /**
     * Set the thread id
     *
     * @param threadId thread id to set
     */
    public static void setThreadId(long threadId) {
        THREAD_LOCAL.set(threadId);
    }

    /**
     * Remove the thread id
     */
    public static void removeThreadId() {
        THREAD_LOCAL.remove();
    }
}
