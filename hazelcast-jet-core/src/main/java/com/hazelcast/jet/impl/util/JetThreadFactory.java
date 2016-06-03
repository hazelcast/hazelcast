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

package com.hazelcast.jet.impl.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread factory for default JET-threads
 */
public class JetThreadFactory implements ThreadFactory {
    private static final AtomicInteger THREAD_COUNTER = new AtomicInteger(1);
    private final String name;
    private final String hzName;

    public JetThreadFactory(String name, String hzName) {
        this.name = name;
        this.hzName = hzName;
    }

    @Override
    public Thread newThread(Runnable r) {
        SecurityManager s = System.getSecurityManager();
        ThreadGroup group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();

        Thread workerThread = new Thread(
                group,
                r,
                this.hzName + "-" + this.name + "-" + THREAD_COUNTER.incrementAndGet()
        );

        workerThread.setDaemon(true);
        return workerThread;
    }
}
