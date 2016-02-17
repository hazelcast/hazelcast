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

package com.hazelcast.map.impl;

import com.hazelcast.map.MapInterceptor;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InterceptorRegistryTest extends HazelcastTestSupport {

    private final InterceptorRegistry registry = new InterceptorRegistry();

    @Test
    public void test_register() throws Exception {
        TestMapInterceptor interceptor = new TestMapInterceptor();

        registry.register(interceptor.id, interceptor);

        List<MapInterceptor> interceptors = registry.getInterceptors();

        assertTrue(interceptors.contains(interceptor));
    }

    @Test
    public void test_deregister() throws Exception {
        TestMapInterceptor interceptor = new TestMapInterceptor();

        registry.register(interceptor.id, interceptor);
        registry.deregister(interceptor.id);

        List<MapInterceptor> interceptors = registry.getInterceptors();

        assertFalse(interceptors.contains(interceptor));
    }


    @Test
    @Category(NightlyTest.class)
    public void testInternalStructuresEmptied_after_concurrent_register_deregister() throws Exception {
        final AtomicBoolean stop = new AtomicBoolean(false);

        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < 10; i++) {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    TestMapInterceptor interceptor = new TestMapInterceptor();
                    while (!stop.get()) {
                        registry.register(interceptor.id, interceptor);
                        registry.deregister(interceptor.id);
                    }
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

        // expecting internals empty.
        assertTrue("Id2Interceptor map should be empty", registry.getId2InterceptorMap().isEmpty());
        assertTrue("Interceptor list should be empty", registry.getInterceptors().isEmpty());
    }


    private static class TestMapInterceptor implements MapInterceptor {

        public final String id = TestMapInterceptor.class.toString();

        @Override
        public Object interceptGet(Object value) {
            return null;
        }

        @Override
        public void afterGet(Object value) {

        }

        @Override
        public Object interceptPut(Object oldValue, Object newValue) {
            return null;
        }

        @Override
        public void afterPut(Object value) {

        }

        @Override
        public Object interceptRemove(Object removedValue) {
            return null;
        }

        @Override
        public void afterRemove(Object value) {

        }

        @Override
        public int hashCode() {
            return id != null ? id.hashCode() : 0;
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

            return id != null ? id.equals(that.id) : that.id == null;

        }
    }
}