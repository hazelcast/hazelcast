/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl.executor;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.StandardLoggerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class ParallelExecutorServiceImplTest {

    private ThreadPoolExecutor executorService;
    private ParallelExecutorService parallelExecutorService;

    @Before
    public void setUp() {
        ILogger logger = new StandardLoggerFactory().getLogger(ParallelExecutorServiceImplStressTest.class.getName());
        executorService = new ThreadPoolExecutor(10, 10, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        parallelExecutorService = new ParallelExecutorService(logger, executorService);
    }

    @After
    public void tearDown() throws InterruptedException {
        executorService.shutdownNow();
        assertTrue("ExecutorService failed to terminate within timeout window", executorService.awaitTermination(10, TimeUnit.SECONDS));
    }

    @Test
    public void testExecuteWithIllegalArguments() {
        ParallelExecutor executor = parallelExecutorService.newParallelExecutor(2);
        try {
            executor.execute(null);
            fail();
        } catch (NullPointerException expected) {
        }
        try {
            executor.execute(null, 1);
            fail();
        } catch (NullPointerException expected) {
        }
    }

    @Test
    public void testHashBasedExecution() throws InterruptedException {
        ParallelExecutor executor = parallelExecutorService.newParallelExecutor(100);
        class Counter {
            /*volatile*/ int count = 0;
        }
        final Counter counter = new Counter() ;
        final int total = 100000;
        final CountDownLatch latch = new CountDownLatch(total);
        final AtomicBoolean b = new AtomicBoolean(false);
        for (int i = 0; i < total; i++) {
            executor.execute(new Runnable() {
                public void run() {
                    counter.count++;
                    latch.countDown();
                }
            }, 0);
        }
        latch.await();
        assertEquals(total, counter.count);
    }
}
