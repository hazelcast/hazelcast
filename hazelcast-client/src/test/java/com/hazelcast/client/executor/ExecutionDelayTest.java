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
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ExecutionDelayTest {

    private static final int NODES = 3;
    private final List<HazelcastInstance> hzs = new ArrayList<HazelcastInstance>(NODES);

    @Before
    public void init() {
        for (int i = 0; i < NODES; i++) {
            hzs.add(Hazelcast.newHazelcastInstance());
        }
    }

    @After
    public void destroy() throws InterruptedException {
        Hazelcast.shutdownAll();
        HazelcastClient.shutdownAll();
    }

    @Test
    public void testExecutorOneNodeFailsUnexpectedly() throws InterruptedException {
        int executions = 20;
        ScheduledExecutorService ex = Executors.newSingleThreadScheduledExecutor();
        try {
            ex.schedule(new Runnable() {
                @Override
                public void run() {
                    hzs.get(1).getLifecycleService().terminate();
                }
            }, 1000, TimeUnit.MILLISECONDS);

            Task task = new Task();
            long time = runClient(task, executions);
            assertTrue("was " + time, time < 5000);
            //        assertEquals(executions, task.getCounterValue());
        } finally {
            ex.shutdown();
        }
    }

    @Test
    public void testExecutorOneNodeShutdown() throws InterruptedException {
        int executions = 20;
        ScheduledExecutorService ex = Executors.newSingleThreadScheduledExecutor();
        try {
            ex.schedule(new Runnable() {
                @Override
                public void run() {
                    hzs.get(1).getLifecycleService().shutdown();
                }
            }, 1000, TimeUnit.MILLISECONDS);

            Task task = new Task();
            long time = runClient(task, executions);
            assertTrue("was " + time, time < 5000);
            //        assertEquals(executions, task.getCounterValue());
        } finally {
            ex.shutdown();
        }
    }

    private long runClient(Task task, int executions) throws InterruptedException {
        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        IExecutorService executor = client.getExecutorService("executor");
        boolean fl;
        long maxTime = 0;

        for (int i = 0; i < executions; i++) {
            long start = System.currentTimeMillis();
            fl = false;
            do {
                try {
                    Future<Long> future = executor.submitToKeyOwner(task, i);
                    future.get();
                    fl = true;
                } catch (Exception exception) {
                }
            } while (!fl);
            long time = System.currentTimeMillis() - start;
            if (maxTime < time) {
                maxTime = time;
            }
            //System.out.println(i + ": " + time + " mls");
            Thread.sleep(100);
        }
        client.getLifecycleService().shutdown();
        return maxTime;
    }

    private static class Task implements Serializable, Callable<Long> {
        final AtomicInteger counter = new AtomicInteger();

        public int getCounterValue() {
            return counter.get();
        }

        @Override
        public Long call() throws Exception {
            long start = System.currentTimeMillis();
            //do something
            try {
                Thread.sleep(100);
                counter.incrementAndGet();
            } catch (InterruptedException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            return System.currentTimeMillis() - start;
        }
    }
}
