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

package com.hazelcast.executor.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.partition.PartitionAware;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.Future;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DistributedExecutorServiceTest extends HazelcastTestSupport {

    private static final String EXECUTOR_NAME = "executor-test";

    private HazelcastInstance hz;
    private DistributedExecutorService distributedExecutorService;

    @Before
    public void setup() {
        Config config = new Config();
        config.addExecutorConfig(new ExecutorConfig().setName(EXECUTOR_NAME).setStatisticsEnabled(false));

        hz = createHazelcastInstance(config);
        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        distributedExecutorService = nodeEngine.getService(DistributedExecutorService.SERVICE_NAME);
    }

    @Test
    public void testExecutorConfigCache_whenExecutorProxyCreated_thenNoConfigCached() {
        IExecutorService executor = hz.getExecutorService(EXECUTOR_NAME);
        assertTrue("Executor config cache should still be empty", distributedExecutorService.executorConfigCache.isEmpty());
    }

    @Test
    public void testExecutorConfigCache_whenTaskSubmitted_thenConfigCached() throws Exception {
        IExecutorService executorService = hz.getExecutorService(EXECUTOR_NAME);
        Future future = executorService.submit(new EmptyRunnable());
        future.get();
        assertEquals("Executor config cache should have cached one element", 1,
                distributedExecutorService.executorConfigCache.size());
    }

    @Test
    public void testExecutorConfigCache_whenSecondTaskSubmitted_thenCachedConfigIsSame() throws Exception {
        IExecutorService executorService = hz.getExecutorService(EXECUTOR_NAME);
        Future future = executorService.submit(new EmptyRunnable());
        future.get();
        ExecutorConfig cachedConfig = distributedExecutorService.executorConfigCache.get(EXECUTOR_NAME);
        future = executorService.submit(new EmptyRunnable());
        future.get();
        assertEquals("Executor config cache should have cached one element", 1,
                distributedExecutorService.executorConfigCache.size());
        assertSame("Executor config cache should have reused the same ExecutorConfig", cachedConfig,
                distributedExecutorService.executorConfigCache.get(EXECUTOR_NAME));
    }

    @Test
    public void testExecutorConfigCache_whenUsedExecutorShutdown_thenConfigRemoved() throws Exception {
        final IExecutorService executorService = hz.getExecutorService(EXECUTOR_NAME);
        Future future = executorService.submit(new EmptyRunnable());
        future.get();
        executorService.shutdown();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(executorService.isShutdown());
            }
        });
        assertTrue("Executor config cache should not contain cached configuration for executor that was already shutdown",
                distributedExecutorService.executorConfigCache.isEmpty());
    }

    static class EmptyRunnable implements Runnable, Serializable, PartitionAware {

        @Override
        public void run() {
        }

        @Override
        public Object getPartitionKey() {
            return "key";
        }
    }
}
