/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.tcp.nonblocking.iobalancer;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.EmptyStatement;

import java.util.concurrent.TimeUnit;

class IOBalancerThread extends Thread {
    private static final String THREAD_NAME_PREFIX = "IOBalancerThread";

    private final IOBalancer ioBalancer;
    private final ILogger log;
    private final int balancerIntervalSeconds;
    private volatile boolean shutdown;

    IOBalancerThread(IOBalancer ioBalancer, int balancerIntervalSeconds,
                     HazelcastThreadGroup threadGroup, ILogger log) {
        super(threadGroup.getInternalThreadGroup(), threadGroup.getThreadNamePrefix(THREAD_NAME_PREFIX));
        this.ioBalancer = ioBalancer;
        this.log = log;
        this.balancerIntervalSeconds = balancerIntervalSeconds;
    }

    void shutdown() {
        shutdown = true;
        interrupt();
    }

    @Override
    public void run() {
        try {
            log.finest("Starting IOBalancer thread");
            while (!shutdown) {
                ioBalancer.checkReadHandlers();
                ioBalancer.checkWriteHandlers();
                TimeUnit.SECONDS.sleep(balancerIntervalSeconds);
            }
        } catch (InterruptedException e) {
            log.finest("IOBalancer thread stopped");
            //this thread is about to exit, no reason restoring the interrupt flag
            EmptyStatement.ignore(e);
        } catch (Throwable e) {
            log.severe("IOBalancer failed", e);
        }
    }
}
