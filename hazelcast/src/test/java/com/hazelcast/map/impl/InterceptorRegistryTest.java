/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl;

import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.MapInterceptorAdaptor;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationexecutor.impl.OperationQueueImpl;
import com.hazelcast.spi.impl.operationexecutor.impl.OperationQueue;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serial;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InterceptorRegistryTest extends HazelcastTestSupport {

    private static final ILogger LOGGER = Logger.getLogger(InterceptorRegistryTest.class);

    private final InterceptorRegistry registry = new InterceptorRegistry();
    private final TestMapInterceptor interceptor = new TestMapInterceptor();

    @Test
    public void testRegister() {
        registry.register(interceptor.id, interceptor);

        assertInterceptorRegistryContainsInterceptor();
    }

    @Test
    public void testRegister_whenRegisteredTwice_doNothing() {
        registry.register(interceptor.id, interceptor);
        registry.register(interceptor.id, interceptor);

        assertInterceptorRegistryContainsInterceptor();
    }

    @Test
    @RequireAssertEnabled
    public void testRegister_fromPartitionOperationThread() throws Exception {
        OperationQueue queue = new OperationQueueImpl();
        PartitionOperationThread thread = getPartitionOperationThread(queue);
        thread.start();

        final CountDownLatch latch = new CountDownLatch(1);
        Object task = (Runnable) () -> {
            try {
                registry.register(interceptor.id, interceptor);
            } catch (AssertionError e) {
                e.printStackTrace();
                latch.countDown();
            }
        };
        queue.add(task, false);

        latch.await();
        thread.shutdown();
        thread.join();

        assertInterceptorRegistryContainsNotInterceptor();
    }

    @Test
    public void testDeregister() {
        registry.register(interceptor.id, interceptor);
        registry.deregister(interceptor.id);

        assertInterceptorRegistryContainsNotInterceptor();
    }

    @Test
    public void testDeregister_whenInterceptorWasNotRegistered_thenDoNothing() {
        registry.deregister(interceptor.id);

        assertInterceptorRegistryContainsNotInterceptor();
    }

    @Test
    @RequireAssertEnabled
    public void testDeregister_fromPartitionOperationThread() throws Exception {
        OperationQueue queue = new OperationQueueImpl();
        PartitionOperationThread thread = getPartitionOperationThread(queue);
        thread.start();

        registry.register(interceptor.id, interceptor);

        final CountDownLatch latch = new CountDownLatch(1);
        Object task = (Runnable) () -> {
            try {
                registry.deregister(interceptor.id);
            } catch (AssertionError e) {
                e.printStackTrace();
                latch.countDown();
            }
        };
        queue.add(task, false);

        latch.await();
        thread.shutdown();
        thread.join();

        assertInterceptorRegistryContainsInterceptor();
    }

    @Test
    @Category(NightlyTest.class)
    public void test_afterConcurrentRegisterDeregister_thenInternalStructuresAreEmpty() throws Exception {
        final AtomicBoolean stop = new AtomicBoolean();

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Thread thread = new Thread(() -> {
                TestMapInterceptor interceptor = new TestMapInterceptor();
                while (!stop.get()) {
                    registry.register(interceptor.id, interceptor);
                    registry.deregister(interceptor.id);
                }
            });
            thread.start();
            threads.add(thread);
        }

        sleepSeconds(10);
        stop.set(true);

        for (Thread thread : threads) {
            thread.join();
        }

        // expecting internals empty
        assertTrue("Interceptor list should be empty", registry.getInterceptors().isEmpty());
        assertTrue("Id2Interceptor map should be empty", registry.getId2InterceptorMap().isEmpty());
    }

    private void assertInterceptorRegistryContainsInterceptor() {
        List<MapInterceptor> interceptors = registry.getInterceptors();
        assertContains(interceptors, interceptor);

        Map<String, MapInterceptor> id2InterceptorMap = registry.getId2InterceptorMap();
        assertTrue(id2InterceptorMap.containsKey(interceptor.id));
        assertTrue(id2InterceptorMap.containsValue(interceptor));
    }

    private void assertInterceptorRegistryContainsNotInterceptor() {
        List<MapInterceptor> interceptors = registry.getInterceptors();
        assertNotContains(interceptors, interceptor);

        Map<String, MapInterceptor> id2InterceptorMap = registry.getId2InterceptorMap();
        assertFalse(id2InterceptorMap.containsKey(interceptor.id));
        assertFalse(id2InterceptorMap.containsValue(interceptor));
    }

    private PartitionOperationThread getPartitionOperationThread(OperationQueue queue) {
        NodeExtension nodeExtension = mock(NodeExtension.class);

        OperationRunner operationRunner = mock(OperationRunner.class);
        OperationRunner[] operationRunners = new OperationRunner[]{operationRunner};

        return new PartitionOperationThread("threadName", 0, queue, LOGGER, nodeExtension,
                operationRunners, getClass().getClassLoader());
    }

    private static class TestMapInterceptor extends MapInterceptorAdaptor {
        @Serial
        private static final long serialVersionUID = 1L;

        public final String id = TestMapInterceptor.class.toString();

        @Override
        public int hashCode() {
            return id.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TestMapInterceptor that = (TestMapInterceptor) o;
            return id.equals(that.id);
        }
    }
}
