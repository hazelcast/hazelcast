/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hazelcast.internal.util.Preconditions.checkHasText;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.StringUtil.csvReplaceSection;

/**
 * Utility class to manipulate and query thread ID.
 */
public final class ThreadUtil {

    private static final ThreadLocal<Long> THREAD_LOCAL = new ThreadLocal<Long>();
    private static final Pattern THREAD_POOL_NAME_PATTERN = Pattern.compile("^hz\\.\\w+\\.(\\w+)\\.thread-\\d+$");

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

    /**
     * Updates the current thread name if matches pattern hz.hzName.poolName.thread-id to hz.hzName.targetPoolName.thread-id
     *
     * @param targetPoolName the target name of the pool
     * @return the original thread name
     * @throws java.lang.IllegalArgumentException if targetPoolName is null or empty
     */
    public static String updateCurrentThreadPoolName(String targetPoolName) {
        checkHasText(targetPoolName, "pool name can't be empty");
        final String currentThreadName = Thread.currentThread().getName();
        final Matcher matcher = THREAD_POOL_NAME_PATTERN.matcher(currentThreadName);
        if (matcher.matches()) {
            Thread.currentThread().setName(csvReplaceSection(currentThreadName, '.', 2, targetPoolName));
        }
        return currentThreadName;
    }

    public static void assertRunningOnPartitionThread() {
        assert Thread.currentThread().getName().contains("partition-operation");
    }
}
