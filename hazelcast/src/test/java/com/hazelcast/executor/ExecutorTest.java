/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.executor;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.instance.StaticNodeFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class ExecutorTest {

    @Before
    @After
    public void shutdown() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testExecutorServiceStats() throws InterruptedException {
        final IExecutorService executorService = StaticNodeFactory.newInstances(new Config(), 1)[0].getExecutorService("testExecutorServiceStats");
        final int k = 10;
        final CountDownLatch latch = new CountDownLatch(k);
        final int executionTime = 200;
        for (int i = 0; i < k; i++) {
            executorService.execute(new Runnable() {
                public void run() {
                    try {
                        Thread.sleep(executionTime);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    latch.countDown();
                }
            });

        }
        latch.await(2, TimeUnit.MINUTES);
        Assert.assertEquals(k, executorService.getLocalExecutorStats().getTotalStarted());
        Assert.assertEquals(k, executorService.getLocalExecutorStats().getTotalFinished());
        Assert.assertEquals(k, executorService.getLocalExecutorStats().getOperationStats().getStarted());
        Assert.assertEquals(k, executorService.getLocalExecutorStats().getOperationStats().getCompleted());

        Assert.assertTrue(executionTime <= executorService.getLocalExecutorStats().getOperationStats().getAverageExecutionTime());
        Assert.assertTrue(executionTime <= executorService.getLocalExecutorStats().getOperationStats().getMinExecutionTime());
        Assert.assertTrue(executionTime <= executorService.getLocalExecutorStats().getOperationStats().getMaxExecutionTime());

    }
}
