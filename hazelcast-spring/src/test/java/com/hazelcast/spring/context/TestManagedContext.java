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

package com.hazelcast.spring.context;

import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.impl.GroupProperties;
import com.hazelcast.spring.CustomSpringJUnit4ClassRunner;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;

import javax.annotation.Resource;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @mdogan 4/6/12
 */
@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"managedContext-applicationContext-hazelcast.xml"})
public class TestManagedContext {

    static {
        System.setProperty(GroupProperties.PROP_VERSION_CHECK_ENABLED, "false");
    }

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
        Assert.assertEquals(context, v.context);
        Assert.assertEquals(bean, v.someBean);
        Assert.assertTrue(v.init);
    }

    @Test
    public void testDistributedTask() throws ExecutionException, InterruptedException {
        SomeTask task = (SomeTask) context.getBean("someTask");
        Future<Long> f = instance1.getExecutorService("test").submit(task);
        Assert.assertEquals(bean.value, f.get().longValue());

        Future<Long> f2 = (Future<Long>) instance1.getExecutorService("test")
                .submit(new DistributedTask<Long>(new SomeTask()));
        Assert.assertEquals(bean.value, f2.get().longValue());
    }

    @Test
    public void testTransactionalTask() throws ExecutionException, InterruptedException {
        Future f = instance1.getExecutorService("test").submit(new DistributedTask(new SomeTransactionalTask(),
                instance2.getCluster().getLocalMember()));
        f.get();
        Assert.assertTrue("transaction manager could not proxy the submitted task.",
                transactionManager.isCommitted());
    }

    @Test
    public void testRunnableTask() throws ExecutionException, InterruptedException {
        Future<?> future = instance1.getExecutorService("test").submit(new SomeRunnableTask());
        future.get();
    }

    @Test
    public void testTransactionalRunnableTask() throws ExecutionException, InterruptedException {
        Future<?> future = instance1.getExecutorService("test").submit(new SomeTransactionalRunnableTask());
        future.get();
        Assert.assertTrue("transaction manager could not proxy the submitted task.",
                transactionManager.isCommitted());
    }

}
