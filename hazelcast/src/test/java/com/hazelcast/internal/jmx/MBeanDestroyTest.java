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

package com.hazelcast.internal.jmx;

import com.hazelcast.collection.IList;
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ISet;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.map.IMap;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.ITopic;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MBeanDestroyTest extends HazelcastTestSupport {

    private MBeanDataHolder holder;

    @Before
    public void setUp() throws Exception {
        holder = new MBeanDataHolder(createHazelcastInstanceFactory(1));
    }

    @Test
    public void testMap() throws Exception {
        IMap map = holder.getHz().getMap("map");
        map.size();
        holder.assertMBeanExistEventually("IMap", map.getName());

        destroyObjectAndAssert(map, "IMap");
    }

    @Test
    public void testMultiMap() throws Exception {
        MultiMap map = holder.getHz().getMultiMap("multimap");
        map.size();
        holder.assertMBeanExistEventually("MultiMap", map.getName());

        destroyObjectAndAssert(map, "MultiMap");
    }

    @Test
    public void testTopic() throws Exception {
        ITopic<String> topic = holder.getHz().getTopic("topic");
        topic.publish("foo");
        holder.assertMBeanExistEventually("ITopic", topic.getName());

        destroyObjectAndAssert(topic, "ITopic");
    }

    @Test
    public void testList() throws Exception {
        IList list = holder.getHz().getList("list");
        list.size();
        holder.assertMBeanExistEventually("IList", list.getName());

        destroyObjectAndAssert(list, "IList");
    }

    @Test
    public void testSet() throws Exception {
        ISet set = holder.getHz().getSet("set");
        set.size();
        holder.assertMBeanExistEventually("ISet", set.getName());

        destroyObjectAndAssert(set, "ISet");
    }

    @Test
    public void testQueue() throws Exception {
        IQueue queue = holder.getHz().getQueue("queue");
        queue.size();
        holder.assertMBeanExistEventually("IQueue", queue.getName());

        destroyObjectAndAssert(queue, "IQueue");
    }

    @Test
    public void testExecutor() throws Exception {
        IExecutorService executor = holder.getHz().getExecutorService("executor");
        executor.submit(new DummyRunnable()).get();
        holder.assertMBeanExistEventually("IExecutorService", executor.getName());

        destroyObjectAndAssert(executor, "IExecutorService");
    }

    @Test
    public void testReplicatedMap() throws Exception {
        String replicatedMapName = randomString();
        ReplicatedMap replicatedMap = holder.getHz().getReplicatedMap(replicatedMapName);
        replicatedMap.size();
        holder.assertMBeanExistEventually("ReplicatedMap", replicatedMap.getName());

        destroyObjectAndAssert(replicatedMap, "ReplicatedMap");
    }

    private void destroyObjectAndAssert(DistributedObject distributedObject, String type) {
        distributedObject.destroy();

        holder.assertMBeanNotExistEventually(type, distributedObject.getName());
    }

    private static class DummyRunnable implements Runnable, Serializable {

        @Override
        public void run() {
        }
    }
}
