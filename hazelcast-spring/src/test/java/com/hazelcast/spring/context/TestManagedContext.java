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

package com.hazelcast.spring.context;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.spring.CustomSpringJUnit4ClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.ExceptionUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;

import javax.annotation.Resource;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author mdogan 4/6/12
 */
@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"managedContext-applicationContext-hazelcast.xml"})
@Category(QuickTest.class)
public class TestManagedContext {

    @Resource(name = "instance1")
    private HazelcastInstance instance1;

    @Resource(name = "instance2")
    private HazelcastInstance instance2;

    @Autowired
    private ApplicationContext context;

    @Autowired
    private DummyTransactionManager transactionManager;

    @Autowired
    private SomeBean bean;

    @BeforeClass
    @AfterClass
    public static void start() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testSerialization() throws InterruptedException {
        instance1.getMap("test").put(1L, new SomeValue());
        SomeValue v = (SomeValue) instance1.getMap("test").get(1L);
        Assert.assertNotNull(v.context);
        Assert.assertNotNull(v.someBean);
        assertEquals(context, v.context);
        assertEquals(bean, v.someBean);
        assertTrue(v.init);
    }

    @Test
    public void testDistributedTask() throws ExecutionException, InterruptedException {
        SomeTask task = (SomeTask) context.getBean("someTask");
        Future<Long> f = instance1.getExecutorService("test").submit(task);
        assertEquals(bean.value, f.get().longValue());

        Future<Long> f2 = instance1.getExecutorService("test").submitToMember(new SomeTask(),
                instance2.getCluster().getLocalMember());
        assertEquals(bean.value, f2.get().longValue());
    }

    @Test
    public void testTransactionalTask() throws ExecutionException, InterruptedException {
        Future f = instance1.getExecutorService("test").submitToMember(new SomeTransactionalTask(),
                instance2.getCluster().getLocalMember());
        f.get();
        assertTrue("transaction manager could not proxy the submitted task.",
                transactionManager.isCommitted());
    }

    @Test
    public void testRunnableTask() throws ExecutionException, InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Throwable> error = new AtomicReference<Throwable>();

        instance1.getExecutorService("test").submitToMember(new SomeRunnableTask(),
                instance2.getCluster().getLocalMember(), new ExecutionCallback() {
            public void onResponse(Object response) {
                latch.countDown();
            }

            public void onFailure(Throwable t) {
                error.set(t);
                latch.countDown();
            }
        });

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        Throwable t = error.get();
        if (t != null) {
            ExceptionUtil.sneakyThrow(t);
        }
    }

    @Test
    public void testTransactionalRunnableTask() throws ExecutionException, InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        instance1.getExecutorService("test").submitToMember(new SomeTransactionalRunnableTask(),
                instance2.getCluster().getLocalMember(), new ExecutionCallback() {
            public void onResponse(Object response) {
                latch.countDown();
            }

            public void onFailure(Throwable t) {
            }
        });
        latch.await(1, TimeUnit.MINUTES);
        assertTrue("transaction manager could not proxy the submitted task.",
                transactionManager.isCommitted());
    }

    @Test
    public void testEntryProcessor() {
        final IMap<Object,Object> map = instance1.getMap("testEntryProcessor");
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        map.put("key4", "value4");
        map.put("key5", "value5");

        final Map<Object,Object> objectMap = map.executeOnEntries(new SomeEntryProcessor());
        assertEquals(5, objectMap.size());
        for (Object o : objectMap.values()) {
            assertEquals("notNull", o);
        }

        final Object result = map.executeOnKey("key8", new SomeEntryProcessor());
        assertEquals("notNull", result);
    }
}
