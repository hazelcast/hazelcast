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

package com.hazelcast.partition;

import com.hazelcast.cluster.Member;
import com.hazelcast.collection.IList;
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ISet;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.config.Config;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.map.IMap;
import com.hazelcast.partition.strategy.StringAndPartitionAwarePartitioningStrategy;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionControlledIdTest extends HazelcastTestSupport {

    private static HazelcastInstance[] instances;

    @BeforeClass
    public static void startHazelcastInstances() {
        Config config = new Config();
        PartitioningStrategy partitioningStrategy = StringAndPartitionAwarePartitioningStrategy.INSTANCE;
        config.getMapConfig("default")
                .setPartitioningStrategyConfig(new PartitioningStrategyConfig(partitioningStrategy));

        TestHazelcastInstanceFactory instanceFactory = new TestHazelcastInstanceFactory(4);
        instances = instanceFactory.newInstances(config);

        warmUpPartitions(instances);
    }

    @AfterClass
    public static void killHazelcastInstances() {
        HazelcastInstanceFactory.terminateAll();
    }

    private HazelcastInstance getHazelcastInstance(String partitionKey) {
        Partition partition = instances[0].getPartitionService().getPartition(partitionKey);
        Member owner = partition.getOwner();
        assertNotNull(owner);

        HazelcastInstance hz = null;
        for (HazelcastInstance instance : instances) {
            if (instance.getCluster().getLocalMember().equals(owner)) {
                hz = instance;
                break;
            }
        }
        assertNotNull(hz);
        return hz;
    }

    public NodeEngineImpl getNodeEngine(HazelcastInstance hz) {
        Node node = getNode(hz);
        return node.nodeEngine;
    }

    @Test
    public void testRingbuffer() {
        String partitionKey = "hazelcast";
        HazelcastInstance hz = getHazelcastInstance(partitionKey);

        Ringbuffer<String> ringbuffer = hz.getRingbuffer("ringbuffer@" + partitionKey);
        ringbuffer.add("foo");
        assertEquals("ringbuffer@" + partitionKey, ringbuffer.getName());
        assertEquals(partitionKey, ringbuffer.getPartitionKey());

        RingbufferService service = getNodeEngine(hz).getService(RingbufferService.SERVICE_NAME);
        final Map<ObjectNamespace, RingbufferContainer> partitionContainers =
                service.getContainers().get(service.getRingbufferPartitionId(ringbuffer.getName()));
        assertNotNull(partitionContainers);
        assertTrue(partitionContainers.containsKey(RingbufferService.getRingbufferNamespace(ringbuffer.getName())));
    }

    @Test
    public void testQueue() {
        String partitionKey = "hazelcast";
        HazelcastInstance hz = getHazelcastInstance(partitionKey);

        IQueue<String> queue = hz.getQueue("queue@" + partitionKey);
        queue.add("");
        assertEquals("queue@" + partitionKey, queue.getName());
        assertEquals(partitionKey, queue.getPartitionKey());

        QueueService service = getNodeEngine(hz).getService(QueueService.SERVICE_NAME);
        assertTrue(service.containsQueue(queue.getName()));
    }

    @Test
    public void testList() {
        String partitionKey = "hazelcast";
        HazelcastInstance hz = getHazelcastInstance(partitionKey);

        IList<String> list = hz.getList("list@" + partitionKey);
        list.add("");
        assertEquals("list@" + partitionKey, list.getName());
        assertEquals(partitionKey, list.getPartitionKey());

        ListService service = getNodeEngine(hz).getService(ListService.SERVICE_NAME);
        assertTrue(service.getContainerMap().containsKey(list.getName()));
    }

    @Test
    public void testSet() {
        String partitionKey = "hazelcast";
        HazelcastInstance hz = getHazelcastInstance(partitionKey);

        ISet<String> set = hz.getSet("set@" + partitionKey);
        set.add("");
        assertEquals("set@" + partitionKey, set.getName());
        assertEquals(partitionKey, set.getPartitionKey());

        SetService service = getNodeEngine(hz).getService(SetService.SERVICE_NAME);
        assertTrue(service.getContainerMap().containsKey(set.getName()));
    }

    @Test
    public void testObjectWithPartitionKeyAndTask() throws Exception {
        HazelcastInstance instance = instances[0];
        IExecutorService executorServices = instance.getExecutorService("executor");
        String partitionKey = "hazelcast";
        IList<Object> list = instance.getList("foobar@" + partitionKey);
        list.add("value");
        ContainsListTask task = new ContainsListTask(list.getName());
        Future<Boolean> future = executorServices.submitToKeyOwner(task, list.getPartitionKey());
        assertTrue(future.get());
    }

    private static class ContainsListTask implements Callable<Boolean>, HazelcastInstanceAware, Serializable {

        private final String name;

        private transient HazelcastInstance hz;

        private ContainsListTask(String name) {
            this.name = name;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hz = hazelcastInstance;
        }

        @Override
        public Boolean call() {
            NodeEngineImpl nodeEngine = getNode(hz).nodeEngine;
            ListService service = nodeEngine.getService(ListService.SERVICE_NAME);
            return service.getContainerMap().containsKey(name);
        }
    }

    @Test
    public void testObjectWithPartitionKeyAndMap() throws Exception {
        HazelcastInstance instance = instances[0];
        IExecutorService executorServices = instance.getExecutorService("executor");
        String partitionKey = "hazelcast";
        String mapKey = "key@" + partitionKey;
        IMap<String, String> map = instance.getMap("map");

        map.put(mapKey, "foobar");

        IList<Object> list = instance.getList("s@" + partitionKey);
        list.add("value");

        ContainsListAndMapEntryTask task = new ContainsListAndMapEntryTask(list.getName(), mapKey);
        Map<Member, Future<Boolean>> futures = executorServices.submitToAllMembers(task);

        int count = 0;
        for (Future<Boolean> future : futures.values()) {
            count += future.get() ? 1 : 0;
        }

        assertEquals(1, count);
    }

    private static class ContainsListAndMapEntryTask implements Callable<Boolean>, HazelcastInstanceAware, Serializable {

        private final String name;
        private final String mapKey;

        private transient HazelcastInstance hz;

        private ContainsListAndMapEntryTask(String name, String mapKey) {
            this.name = name;
            this.mapKey = mapKey;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hz = hazelcastInstance;
        }

        @Override
        public Boolean call() {
            NodeEngineImpl nodeEngine = getNode(hz).nodeEngine;
            ListService service = nodeEngine.getService(ListService.SERVICE_NAME);

            IMap map = hz.getMap("map");
            return map.localKeySet().contains(mapKey) && service.getContainerMap().containsKey(name);
        }
    }
}
