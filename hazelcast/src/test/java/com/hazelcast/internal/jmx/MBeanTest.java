/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.jmx;

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;

/**
 * This is the test that just tests if the mbeans get created. Tests in this class only exist for types that don't have
 * a specific test in this package. See LockMBeanTest for a specific test.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MBeanTest extends HazelcastTestSupport {

    private MBeanDataHolder holder;

    @Before
    public void setUp() throws Exception {
        holder = new MBeanDataHolder(createHazelcastInstanceFactory(1));
    }

    @Test
    public void testAtomicLong() throws Exception {
        IAtomicLong atomicLong = holder.getHz().getAtomicLong("atomiclong");
        atomicLong.incrementAndGet();

        holder.assertMBeanExistEventually("IAtomicLong", atomicLong.getName());
    }

    @Test
    public void testAtomicReference() throws Exception {
        IAtomicReference<String> atomicReference = holder.getHz().getAtomicReference("atomicreference");
        atomicReference.set(null);

        holder.assertMBeanExistEventually("IAtomicReference", atomicReference.getName());
    }

    @Test
    public void testSemaphore() throws Exception {
        ISemaphore semaphore = holder.getHz().getSemaphore("semaphore");
        semaphore.availablePermits();

        holder.assertMBeanExistEventually("ISemaphore", semaphore.getName());
    }

    @Test
    public void testConnection() throws Exception {
        holder.assertMBeanExistEventually("HazelcastInstance.ConnectionManager", holder.getHz().getName());
    }

    @Test
    public void testCountDownLatch() throws Exception {
        ICountDownLatch countDownLatch = holder.getHz().getCountDownLatch("semaphore");
        countDownLatch.getCount();

        holder.assertMBeanExistEventually("ICountDownLatch", countDownLatch.getName());
    }

    @Test
    public void testMap() throws Exception {
        IMap map = holder.getHz().getMap("map");
        map.size();

        holder.assertMBeanExistEventually("IMap", map.getName());
    }

    @Test
    public void testMultiMap() throws Exception {
        MultiMap map = holder.getHz().getMultiMap("multimap");
        map.size();

        holder.assertMBeanExistEventually("MultiMap", map.getName());
    }

    @Test
    public void testTopic() throws Exception {
        ITopic<String> topic = holder.getHz().getTopic("topic");
        topic.publish("foo");

        holder.assertMBeanExistEventually("ITopic", topic.getName());
    }

    @Test
    public void testList() throws Exception {
        IList list = holder.getHz().getList("list");
        list.size();

        holder.assertMBeanExistEventually("IList", list.getName());
    }

    @Test
    public void testSet() throws Exception {
        ISet set = holder.getHz().getSet("set");
        set.size();

        holder.assertMBeanExistEventually("ISet", set.getName());
    }

    @Test
    public void testQueue() throws Exception {
        IQueue queue = holder.getHz().getQueue("queue");
        queue.size();

        holder.assertMBeanExistEventually("IQueue", queue.getName());
    }

    @Test
    public void testExecutor() throws Exception {
        IExecutorService executor = holder.getHz().getExecutorService("executor");
        executor.submit(new DummyRunnable()).get();

        holder.assertMBeanExistEventually("IExecutorService", executor.getName());
    }

    @Test
    public void testReplicatedMap() throws Exception {
        String replicatedMapName = randomString();
        ReplicatedMap replicatedMap = holder.getHz().getReplicatedMap(replicatedMapName);
        replicatedMap.size();

        holder.assertMBeanExistEventually("ReplicatedMap", replicatedMap.getName());
    }

    private static class DummyRunnable implements Runnable, Serializable {

        @Override
        public void run() {
        }
    }
}
