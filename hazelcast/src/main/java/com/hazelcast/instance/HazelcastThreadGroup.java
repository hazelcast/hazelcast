/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance;

import com.hazelcast.logging.ILogger;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * A wrapper around the {@link java.lang.ThreadGroup} that provides some additional capabilities. It is a grouping of
 * all thread creational logic throughout the system. To access the actual ThreadGroup, call {@link #getInternalThreadGroup()}.
 */
public final class HazelcastThreadGroup {

    private final ILogger logger;
    private final ThreadGroup internalThreadGroup;
    private final ClassLoader classLoader;
    private final String hzName;

    public HazelcastThreadGroup(String name, ILogger logger, ClassLoader classLoader) {
        this.hzName = name;
        this.internalThreadGroup = new ThreadGroup(name);
        this.logger = logger;
        this.classLoader = classLoader;
    }

    /**
     * Gets the threadname prefix.
     *
     * @param name the basic name of the thread.
     * @return the created threadname prefix.
     * @throws java.lang.NullPointerException if name is null.
     */
    public String getThreadNamePrefix(String name) {
        checkNotNull(name, "name can't be null");
        return "hz." + hzName + "." + name;
    }

    /**
     * Gets the threadpool prefix for a given poolname.
     *
     * @param poolName the name of the pool.
     * @return the threadpool prefix.
     * @throws java.lang.NullPointerException if poolname is null.
     */
    public String getThreadPoolNamePrefix(String poolName) {
        return getThreadNamePrefix(poolName) + ".thread-";
    }

    /**
     * Returns the ClassLoader used by threads of this HazelcastThreadGroup.
     *
     * @return the ClassLoader.
     */
    public ClassLoader getClassLoader() {
        return classLoader;
    }

    /**
     * Gets the internal ThreadGroup; so the actual ThreadGroup object.
     *
     * @return the internal ThreadGroup.
     */
    public ThreadGroup getInternalThreadGroup() {
        return internalThreadGroup;
    }

    /**
     * Destroys all threads in this ThreadGroup.
     */
    public void destroy() {
        int numThreads = internalThreadGroup.activeCount();
        Thread[] threads = new Thread[numThreads * 2];
        numThreads = internalThreadGroup.enumerate(threads, false);
        for (int i = 0; i < numThreads; i++) {
            Thread thread = threads[i];
            if (!thread.isAlive()) {
                continue;
            }
            if (logger.isFinestEnabled()) {
                logger.finest("Shutting down thread " + thread.getName());
            }
            thread.interrupt();
        }
    }
}
