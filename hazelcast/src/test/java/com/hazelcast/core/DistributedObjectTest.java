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

package com.hazelcast.core;

import com.hazelcast.config.Config;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * @author mdogan 8/25/13
 */
@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class DistributedObjectTest extends HazelcastTestSupport {

    private HazelcastInstance instance;

    @Before
    public void setUp() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        Config config = new Config();
        instance = factory.newHazelcastInstance(config);
    }

    @Test
    public void testMap() {
        DistributedObject object = instance.getMap("test");
        test(object);
    }

    @Test
    public void testQueue() {
        DistributedObject object = instance.getQueue("test");
        test(object);
    }

    @Test
    public void testTopic() {
        DistributedObject object = instance.getTopic("test");
        test(object);
    }

    @Test
    public void testMultiMap() {
        DistributedObject object = instance.getMultiMap("test");
        test(object);
    }

    @Test
    @Ignore
    public void testSet() {
        DistributedObject object = instance.getSet("test");
        test(object);
    }

    @Test
    @Ignore
    public void testList() {
        DistributedObject object = instance.getList("test");
        test(object);
    }

    @Test
    public void testExecutorService() {
        DistributedObject object = instance.getExecutorService("test");
        test(object);
    }

    @Test
    public void testLock() {
        DistributedObject object = instance.getLock("test");
        test(object);
    }

    @Test
    public void testLock2() {
        DistributedObject object = instance.getLock(System.currentTimeMillis());
        test(object);
    }

    @Test
    public void testAtomicLong() {
        DistributedObject object = instance.getAtomicLong("test");
        test(object);
    }

    @Test
    public void testSemaphore() {
        DistributedObject object = instance.getSemaphore("test");
        test(object);
    }

    @Test
    public void testCountdownLatch() {
        DistributedObject object = instance.getCountDownLatch("test");
        test(object);
    }

    @Test
    public void testIdGenerator() {
        DistributedObject object = instance.getIdGenerator("test");
        test(object);
    }

    private void test(DistributedObject object) {
        DistributedObject object2 = instance.getDistributedObject(object.getServiceName(), object.getId());
        Assert.assertEquals(object.getServiceName(), object2.getServiceName());
        Assert.assertEquals(object.getName(), object2.getName());
        Assert.assertEquals(object.getId(), object2.getId());
        Assert.assertEquals(object, object2);
        Assert.assertTrue(instance.getDistributedObjects().contains(object));
    }
}
