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

package com.hazelcast.internal.util;

import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Utility class to manipulate and query thread ID.
 */
public final class ThreadUtil {

    private static final ThreadLocal<Long> THREAD_LOCAL = new ThreadLocal<Long>();

    private ThreadUtil() {
    }

    /**
     * Get the thread ID.
     *
     * @return the thread ID
     */
    public static long getThreadId() {
        final Long threadId = THREAD_LOCAL.get();
        if (threadId != null) {
            return threadId;
        }
        return Thread.currentThread().getId();
    }

    /**
     * Set the thread ID.
     *
     * @param threadId thread ID to set
     */
    public static void setThreadId(long threadId) {
        THREAD_LOCAL.set(threadId);
    }

    /**
     * Remove the thread ID.
     */
    public static void removeThreadId() {
        THREAD_LOCAL.remove();
    }


    /**
     * Creates the threadname with prefix and notation.
     *
     * @param hzName the name of the hazelcast instance
     * @param name   the basic name of the thread
     * @return the threadname .
     * @throws java.lang.NullPointerException if name is null.
     */
    public static String createThreadName(String hzName, String name) {
        checkNotNull(name, "name can't be null");
        return "hz." + hzName + "." + name;
    }

    /**
     * Creates the threadpool name with prefix and notation.
     *
     * @param hzName   the name of the hazelcast instance
     * @param poolName the name of the pool.
     * @return the threadpool name.
     * @throws java.lang.NullPointerException if poolname is null.
     */
    public static String createThreadPoolName(String hzName, String poolName) {
        return createThreadName(hzName, poolName) + ".thread-";
    }

    public static void assertRunningOnPartitionThread() {
        assert Thread.currentThread() instanceof PartitionOperationThread;
    }

    public static boolean isRunningOnPartitionThread() {
        return Thread.currentThread() instanceof PartitionOperationThread;
    }
}
