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

import java.util.concurrent.ThreadFactory;

public abstract class AbstractExecutorThreadFactory implements ThreadFactory {

    protected final ClassLoader classLoader;
    protected final ThreadGroup threadGroup;

    public AbstractExecutorThreadFactory(ThreadGroup threadGroup, ClassLoader classLoader) {
        this.threadGroup = threadGroup;
        this.classLoader = classLoader;
    }

    public final Thread newThread(Runnable r) {
        final Thread t = createThread(r);
        t.setContextClassLoader(classLoader);
        // If context classloader of created thread is still null
        if (t.getContextClassLoader() == null) {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            // If classloader of current thread is null (means that caller class is loaded by bootloader),
            // at first try to use classloader of current class.
            if (cl == null) {
                cl = getClass().getClassLoader();
            }
            // If classloader is still null (this might not be possible), use system classloader as current classloader
            if (cl == null) {
                cl = ClassLoader.getSystemClassLoader();
            }
            t.setContextClassLoader(cl);
        }
        if (t.isDaemon()) {
            t.setDaemon(false);
        }
        if (t.getPriority() != Thread.NORM_PRIORITY) {
            t.setPriority(Thread.NORM_PRIORITY);
        }
        return t;
    }

    protected abstract Thread createThread(Runnable r);

}
