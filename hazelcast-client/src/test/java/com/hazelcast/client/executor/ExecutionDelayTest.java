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

package com.hazelcast.client.executor;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ExecutionDelayTest extends HazelcastTestSupport {

    private static final int NODES = 3;
    private final List<HazelcastInstance> hzs = new ArrayList<HazelcastInstance>(NODES);
    static final AtomicInteger counter = new AtomicInteger();

    @Before
    public void init() {
        counter.set(0);
        for (int i = 0; i < NODES; i++) {
            hzs.add(Hazelcast.newHazelcastInstance());
        }
    }

    @After
    public void destroy() throws InterruptedException {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testExecutorOneNodeFailsUnexpectedly() throws InterruptedException, ExecutionException {
        final int executions = 20;
        final ScheduledExecutorService ex = Executors.newSingleThreadScheduledExecutor();
        try {
            ex.schedule(new Runnable() {
                @Override
                public void run() {
                    hzs.get(1).getLifecycleService().terminate();
                }
            }, 1000, TimeUnit.MILLISECONDS);

            final Task task = new Task();
            runClient(task, executions);

            assertTrueEventually(new AssertTask() {
                @Override
                public void run() {
                    assertEquals(executions, counter.get());
                }
            });
        } finally {
            ex.shutdown();
        }
    }

    @Test
    public void testExecutorOneNodeShutdown() throws InterruptedException, ExecutionException {
        final int executions = 20;
        final ScheduledExecutorService ex = Executors.newSingleThreadScheduledExecutor();
        try {
            ex.schedule(new Runnable() {
                @Override
                public void run() {
                    hzs.get(1).shutdown();
                }
            }, 1000, TimeUnit.MILLISECONDS);

            final Task task = new Task();
            runClient(task, executions);

            assertTrueEventually(new AssertTask() {
                @Override
                public void run() {
                    assertEquals(executions, counter.get());
                }
            });
        } finally {
            ex.shutdown();
        }
    }

    private void runClient(Task task, int executions) throws InterruptedException, ExecutionException {
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setRedoOperation(true);
        final HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        final IExecutorService executor = client.getExecutorService("executor");
        for (int i = 0; i < executions; i++) {
            final Future future = executor.submitToKeyOwner(task, i);
            future.get();
            Thread.sleep(100);
        }
    }

    private static class Task implements Callable, Serializable {
        @Override
        public Object call() throws Exception {
            counter.incrementAndGet();
            return null;
        }
    }
}
