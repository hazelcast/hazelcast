/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.impl;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class ExecutorThreadFactory implements ThreadFactory {
    private final ThreadGroup group;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;
    private final ClassLoader classLoader;
    private final ThreadCleanup cleanup;

    public ExecutorThreadFactory(final ThreadGroup threadGroup, final String threadNamePrefix, final ClassLoader classLoader) {
        this(threadGroup, threadNamePrefix, classLoader, null);
    }

    public ExecutorThreadFactory(final ThreadGroup threadGroup, final String threadNamePrefix,
                                 final ClassLoader classLoader, ThreadCleanup cleanup) {
        this.group = threadGroup;
        this.classLoader = classLoader;
        this.namePrefix = threadNamePrefix;
        this.cleanup = new CleanupImpl(cleanup);
    }

    public Thread newThread(Runnable r) {
        final Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0) {
            public void run() {
                try {
                    super.run();
                } catch (OutOfMemoryError e) {
                    OutOfMemoryErrorDispatcher.onOutOfMemory(e);
                } finally {
                    cleanup.cleanup(this);
                }
            }
        };
        t.setContextClassLoader(classLoader);
        if (t.isDaemon()) {
            t.setDaemon(false);
        }
        if (t.getPriority() != Thread.NORM_PRIORITY) {
            t.setPriority(Thread.NORM_PRIORITY);
        }
        return t;
    }

    public interface ThreadCleanup {
        void cleanup(final Thread t);
    }

    private class CleanupImpl implements ThreadCleanup {

        private final ThreadCleanup externalCleanup;

        private CleanupImpl(ThreadCleanup externalCleanup) {
            this.externalCleanup = externalCleanup;
        }

        public void cleanup(final Thread t) {
            if (externalCleanup != null) {
                try {
                    externalCleanup.cleanup(t);
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
            try {
                ThreadContext.shutdown(t);
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }
}