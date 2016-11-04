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

package com.hazelcast.jet.impl.executor;


import com.hazelcast.jet.impl.util.JetThreadFactory;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.NodeEngine;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.impl.util.JetUtil.unchecked;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkTrue;

public abstract class AbstractExecutor<T extends Worker> {

    protected final ILogger logger;
    protected final List<T> workers;

    private final String name;
    private final Thread[] threads;
    private final int awaitingTimeOut;
    private final ThreadFactory threadFactory;

    protected AbstractExecutor(String name, int threadNum, int awaitingTimeOut, NodeEngine nodeEngine) {
        checkNotNull(name);
        checkTrue(threadNum > 0, "Max thread count must be greater than zero");

        String hzName = nodeEngine.getHazelcastInstance().getName();

        threadFactory = new JetThreadFactory(name + "-executor", hzName);
        logger = nodeEngine.getLogger(getClass());

        threads = new Thread[threadNum];
        this.awaitingTimeOut = awaitingTimeOut;
        //noinspection unchecked
        workers = new ArrayList<>(threadNum);

        for (int i = 0; i < threads.length; i++) {
            T worker = createWorker();
            workers.add(worker);
            threads[i] = createThread(worker);
        }

        this.name = name;
    }

    protected Thread createThread(Runnable runnable) {
        return threadFactory.newThread(runnable);
    }

    protected abstract T createWorker();

    /**
     * Name of the executor
     *
     * @return the name of the executor
     */
    public String getName() {
        return name;
    }

    /**
     * Synchronously shutdown executor
     */
    public void shutdown() {
        for (Worker worker : workers) {
            try {
                worker.shutdown().get(awaitingTimeOut, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw unchecked(e);
            }
        }
    }

    protected void startWorkers() {
        for (Worker worker : workers) {
            worker.start();
        }
    }

    protected void startThreads() {
        for (Thread thread : threads) {
            thread.start();
        }
    }

    /**
     * Send wakeUp signal to all workers
     */
    public void wakeUp() {
        for (Worker worker : workers) {
            worker.wakeUp();
        }
    }

    @Override
    public String toString() {
        return name;
    }
}
