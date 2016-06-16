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

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkTrue;

public abstract class AbstractExecutorImpl<T extends WorkingProcessor>
        implements AbstractExecutor {

    protected final ILogger logger;
    protected final T[] processors;

    private final String name;
    private final Thread[] workers;
    private final int awaitingTimeOut;
    private final ThreadFactory threadFactory;

    protected AbstractExecutorImpl(String name,
                                   int threadNum,
                                   int awaitingTimeOut,
                                   NodeEngine nodeEngine) {
        checkNotNull(name);
        checkTrue(threadNum > 0, "Max thread count must be greater than zero");

        String hzName = nodeEngine.getHazelcastInstance().getName();

        this.threadFactory = new JetThreadFactory(name + "-executor", hzName);
        this.logger = nodeEngine.getLogger(AbstractLocalTaskExecutorImpl.class);

        this.workers = new Thread[threadNum];
        this.awaitingTimeOut = awaitingTimeOut;
        this.processors = createWorkingProcessors(threadNum);

        for (int i = 0; i < this.workers.length; i++) {
            this.processors[i] = createWorkingProcessor(threadNum);
            this.workers[i] = worker(processors[i]);
        }

        this.name = name;
    }

    protected Thread worker(Runnable processor) {
        return this.threadFactory.newThread(processor);
    }

    protected abstract T createWorkingProcessor(int threadNum);

    protected abstract T[] createWorkingProcessors(int threadNum);

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public void shutdown() throws Exception {
        for (WorkingProcessor processor : this.processors) {
            processor.shutdown().get(this.awaitingTimeOut, TimeUnit.SECONDS);
        }
    }

    protected void startProcessors() {
        for (WorkingProcessor processor : this.processors) {
            processor.start();
        }
    }

    protected void startWorkers() {
        for (Thread worker : this.workers) {
            worker.start();
        }
    }

    @Override
    public void wakeUp() {
        for (WorkingProcessor processor : this.processors) {
            processor.wakeUp();
        }
    }

    @Override
    public String toString() {
        return this.name;
    }
}
