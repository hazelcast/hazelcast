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

package com.hazelcast.map.impl.querycache;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.querycache.subscriber.operation.MadePublishableOperationFactory;
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

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NodeQueryCacheContextTest extends HazelcastTestSupport {

    private QueryCacheContext context;
    private int partitionCount;

    @Before
    public void setUp() {
        HazelcastInstance hz = createHazelcastInstance();
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(hz);

        MapService mapService = nodeEngineImpl.getService(MapService.SERVICE_NAME);
        context = mapService.getMapServiceContext().getQueryCacheContext();

        partitionCount = nodeEngineImpl.getPartitionService().getPartitionCount();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDestroy() {
        context.destroy();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testInvokerWrapper_invokeOnAllPartitions() throws Exception {
        MadePublishableOperationFactory factory = new MadePublishableOperationFactory("mapName", "cacheId");

        Map<Integer, Object> result = (Map<Integer, Object>) context.getInvokerWrapper().invokeOnAllPartitions(factory, false);
        assertEquals(partitionCount, result.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvokerWrapper_invokeOnAllPartitions_whenRequestOfWrongType_thenThrowException() throws Exception {
        context.getInvokerWrapper().invokeOnAllPartitions(new Object(), false);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInvokerWrapper_invoke() {
        context.getInvokerWrapper().invoke(null, false);
    }

    @Test
    public void testGetQueryCacheScheduler() {
        QueryCacheScheduler scheduler = context.getQueryCacheScheduler();
        assertNotNull(scheduler);

        final QuerySchedulerTask task = new QuerySchedulerTask();
        scheduler.execute(task);

        final QuerySchedulerRepetitionTask repetitionTask = new QuerySchedulerRepetitionTask();
        scheduler.scheduleWithRepetition(repetitionTask, 1);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(task.executed);
                assertTrue(repetitionTask.counter.get() > 1);
            }
        });

        scheduler.shutdown();
    }

    public static class QuerySchedulerTask implements Runnable {

        public volatile boolean executed;

        @Override
        public void run() {
            executed = true;
        }
    }

    public static class QuerySchedulerRepetitionTask implements Runnable {

        public final AtomicInteger counter = new AtomicInteger();

        @Override
        public void run() {
            counter.incrementAndGet();
        }
    }
}
