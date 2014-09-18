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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class NamedThreadPoolExecutor extends ThreadPoolExecutor implements ManagedExecutorService {

    private final String name;

    public NamedThreadPoolExecutor(String name, int corePoolSize, int maximumPoolSize, long keepAliveTime,
                                   TimeUnit unit, BlockingQueue<Runnable> workQueue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
        this.name = name;
    }

    public NamedThreadPoolExecutor(String name, int corePoolSize, int maximumPoolSize, long keepAliveTime,
                                   TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
        this.name = name;
    }

    public NamedThreadPoolExecutor(String name, int corePoolSize, int maximumPoolSize, long keepAliveTime,
                                   TimeUnit unit, BlockingQueue<Runnable> workQueue, RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
        this.name = name;
    }

    public NamedThreadPoolExecutor(String name, int corePoolSize, int maximumPoolSize, long keepAliveTime,
                                   TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory,
                                   RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public int getQueueSize() {
        return getQueue().size();
    }

    @Override
    public int getRemainingQueueCapacity() {
        return getQueue().remainingCapacity();
    }

    @Override
    public String toString() {
        return "ThreadPoolExecutor{" + "name='" + name + '\'' + '}';
    }
}
