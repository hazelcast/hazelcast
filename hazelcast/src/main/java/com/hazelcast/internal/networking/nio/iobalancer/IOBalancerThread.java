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

package com.hazelcast.internal.networking.nio.iobalancer;

import com.hazelcast.logging.ILogger;

import java.util.concurrent.BlockingQueue;

import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.internal.util.ThreadUtil.createThreadName;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

class IOBalancerThread extends Thread {
    private static final String THREAD_NAME_PREFIX = "IO.BalancerThread";

    private final IOBalancer ioBalancer;
    private final ILogger log;
    private final long balancerIntervalMs;
    private final BlockingQueue<Runnable> workQueue;
    private volatile boolean shutdown;

    IOBalancerThread(IOBalancer ioBalancer,
                     int balancerIntervalSeconds,
                     String hzName,
                     ILogger log,
                     BlockingQueue<Runnable> workQueue) {
        super(createThreadName(hzName, THREAD_NAME_PREFIX));
        this.ioBalancer = ioBalancer;
        this.log = log;
        this.balancerIntervalMs = SECONDS.toMillis(balancerIntervalSeconds);
        this.workQueue = workQueue;
    }

    void shutdown() {
        shutdown = true;
        interrupt();
    }

    // squid:S2142 is suppressed since Sonar analysis gives false positive for an already handled or known case.
    @SuppressWarnings("squid:S2142")
    @Override
    public void run() {
        try {
            log.finest("Starting IOBalancer thread");
            long nextRebalanceMs = currentTimeMillis() + balancerIntervalMs;
            while (!shutdown) {
                for (; ; ) {
                    long maxPollDurationMs = nextRebalanceMs - currentTimeMillis();
                    Runnable task = maxPollDurationMs <= 0 ? workQueue.poll() : workQueue.poll(maxPollDurationMs, MILLISECONDS);
                    if (task == null) {
                        // we are finished with taking task from the queue, lets
                        // do a bit of rebalancing.
                        break;
                    }
                    task.run();
                }

                ioBalancer.rebalance();
                nextRebalanceMs = currentTimeMillis() + balancerIntervalMs;
            }
        } catch (InterruptedException e) {
            log.finest("IOBalancer thread stopped");
            //this thread is about to exit, no reason restoring the interrupt flag
            ignore(e);
        } catch (Throwable e) {
            log.severe("IOBalancer failed", e);
        }
    }
}
