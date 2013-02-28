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

package com.hazelcast.util.executor;

import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.instance.ThreadContext;

import java.util.concurrent.ThreadFactory;

public abstract class ExecutorThreadFactory implements ThreadFactory {
    private final ClassLoader classLoader;
    private final ThreadGroup threadGroup;

    public ExecutorThreadFactory(ThreadGroup threadGroup, ClassLoader classLoader) {
        this.threadGroup = threadGroup;
        this.classLoader = classLoader;
    }

    public final Thread newThread(Runnable r) {
        final Thread t = createThread(r);
        t.setContextClassLoader(classLoader);
        if (t.isDaemon()) {
            t.setDaemon(false);
        }
        if (t.getPriority() != Thread.NORM_PRIORITY) {
            t.setPriority(Thread.NORM_PRIORITY);
        }
        return t;
    }

    protected Thread createThread(Runnable r) {
        return new ManagedThread(r, newThreadName(), 0);
    }

    protected abstract String newThreadName();

    protected void afterThreadExit(ManagedThread t) {
        ThreadContext.shutdown(t);
    }

    protected final class ManagedThread extends Thread {

        protected final int id;

        public ManagedThread(Runnable target, String name, int id) {
            super(threadGroup, target, name);
            this.id = id;
        }

        public void run() {
            try {
                super.run();
            } catch (OutOfMemoryError e) {
                OutOfMemoryErrorDispatcher.onOutOfMemory(e);
            } finally {
                try {
                    afterThreadExit(this);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}