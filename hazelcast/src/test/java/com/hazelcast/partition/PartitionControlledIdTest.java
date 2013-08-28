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

package com.hazelcast.partition;

import com.hazelcast.collection.CollectionPartitionContainer;
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionProxyType;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.collection.list.ObjectListProxy;
import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.concurrent.lock.InternalLockNamespace;
import com.hazelcast.concurrent.lock.LockServiceImpl;
import com.hazelcast.concurrent.lock.LockStore;
import com.hazelcast.concurrent.semaphore.SemaphoreService;
import com.hazelcast.config.Config;
import com.hazelcast.config.PartitionStrategyConfig;
import com.hazelcast.core.*;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.partition.strategy.StringAndPartitionAwarePartitioningStrategy;
import com.hazelcast.queue.QueueService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author mdogan 8/21/13
 */
@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class PartitionControlledIdTest extends HazelcastTestSupport {

    private HazelcastInstance[] instances;

    @Before
    public void setUp() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        Config config = new Config();
        config.getMapConfig("default").setPartitionStrategyConfig(new PartitionStrategyConfig(new StringAndPartitionAwarePartitioningStrategy()));
        instances = factory.newInstances(config);
        warmUpPartitions(instances);
    }

    private HazelcastInstance getHazelcastInstance(String partitionKey) {
        Partition partition = instances[0].getPartitionService().getPartition(partitionKey);
        Member owner = partition.getOwner();

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
    public void testLock() throws Exception {
        String partitionKey = "hazelcast";
        HazelcastInstance hz = getHazelcastInstance(partitionKey);

        ILock lock = hz.getLock("s@" + partitionKey);
        lock.lock();
        assertEquals("s@" + partitionKey, lock.getName());
        assertEquals(partitionKey, lock.getPartitionKey());

        Node node = getNode(hz);
        LockServiceImpl lockService = node.nodeEngine.getService(LockServiceImpl.SERVICE_NAME);

        Partition partition = instances[0].getPartitionService().getPartition(partitionKey);
        LockStore lockStore = lockService.getLockStore(partition.getPartitionId(), new InternalLockNamespace());
        assertTrue(lockStore.isLocked(node.getSerializationService().toData(lock.getName())));
    }

    @Test
    public void testSemaphore() throws Exception {
        String partitionKey = "hazelcast";
        HazelcastInstance hz = getHazelcastInstance(partitionKey);

        ISemaphore semaphore = hz.getSemaphore("s@" + partitionKey);
        semaphore.release();
        assertEquals("s@" + partitionKey, semaphore.getName());
        assertEquals(partitionKey, semaphore.getPartitionKey());

        SemaphoreService service = getNodeEngine(hz).getService(SemaphoreService.SERVICE_NAME);
        assertTrue(service.containsSemaphore(semaphore.getName()));
    }

    @Test
    public void testIdGenerator() throws Exception {
        String partitionKey = "hazelcast";
        HazelcastInstance hz = getHazelcastInstance(partitionKey);

        IdGenerator idGenerator = hz.getIdGenerator("s@" + partitionKey);
        idGenerator.newId();
        assertEquals("s@" + partitionKey, idGenerator.getName());
        assertEquals(partitionKey, idGenerator.getPartitionKey());

        AtomicLongService service = getNodeEngine(hz).getService(AtomicLongService.SERVICE_NAME);
        assertTrue(service.containsAtomicLong("hz:atomic:idGenerator:" + idGenerator.getName()));
    }

    @Test
    public void testAtomicLong() throws Exception {
        String partitionKey = "hazelcast";
        HazelcastInstance hz = getHazelcastInstance(partitionKey);

        IAtomicLong atomicLong = hz.getAtomicLong("s@" + partitionKey);
        atomicLong.incrementAndGet();
        assertEquals("s@" + partitionKey, atomicLong.getName());
        assertEquals(partitionKey, atomicLong.getPartitionKey());

        AtomicLongService service = getNodeEngine(hz).getService(AtomicLongService.SERVICE_NAME);
        assertTrue(service.containsAtomicLong(atomicLong.getName()));
    }

    @Test
    public void testQueue() throws Exception {
        String partitionKey = "hazelcast";
        HazelcastInstance hz = getHazelcastInstance(partitionKey);

        IQueue queue = hz.getQueue("s@" + partitionKey);
        queue.add("");
        assertEquals("s@" + partitionKey, queue.getName());
        assertEquals(partitionKey, queue.getPartitionKey());

        QueueService service = getNodeEngine(hz).getService(QueueService.SERVICE_NAME);
        assertTrue(service.containsQueue(queue.getName()));
    }

    @Test
    public void testList() throws Exception {
        String partitionKey = "hazelcast";
        HazelcastInstance hz = getHazelcastInstance(partitionKey);

        IList list = hz.getList("s@" + partitionKey);
        list.add("");
        assertEquals("s@" + partitionKey, list.getName());
        assertEquals(partitionKey, list.getPartitionKey());

        CollectionService service = getNodeEngine(hz).getService(CollectionService.SERVICE_NAME);

        Partition partition = instances[0].getPartitionService().getPartition(partitionKey);
        CollectionPartitionContainer cpc = service.getPartitionContainer(partition.getPartitionId());
        assertTrue(cpc.containsCollection(new CollectionProxyId(ObjectListProxy.COLLECTION_LIST_NAME, list.getName(), CollectionProxyType.LIST)));
    }

    @Test
    public void testSet() throws Exception {
        String partitionKey = "hazelcast";
        HazelcastInstance hz = getHazelcastInstance(partitionKey);

        ISet set = hz.getSet("s@" + partitionKey);
        set.add("");
        assertEquals("s@" + partitionKey, set.getName());
        assertEquals(partitionKey, set.getPartitionKey());

        CollectionService service = getNodeEngine(hz).getService(CollectionService.SERVICE_NAME);

        Partition partition = instances[0].getPartitionService().getPartition(partitionKey);
        CollectionPartitionContainer cpc = service.getPartitionContainer(partition.getPartitionId());
        assertTrue(cpc.containsCollection(new CollectionProxyId(ObjectListProxy.COLLECTION_SET_NAME, set.getName(), CollectionProxyType.SET)));
    }

    @Test
    public void testCountDownLatch() throws Exception {
        String partitionKey = "hazelcast";
        HazelcastInstance hz = getHazelcastInstance(partitionKey);

        ICountDownLatch countDownLatch = hz.getCountDownLatch("s@" + partitionKey);
        countDownLatch.trySetCount(1);
        assertEquals("s@" + partitionKey, countDownLatch.getName());
        assertEquals(partitionKey, countDownLatch.getPartitionKey());

        CountDownLatchService service = getNodeEngine(hz).getService(CountDownLatchService.SERVICE_NAME);
        assertTrue(service.containsLatch(countDownLatch.getName()));
    }

    @Test
    public void testObjectWithPartitionKeyAndTask() throws Exception {
        HazelcastInstance instance = instances[0];
        IExecutorService executorServices = instance.getExecutorService("executor");
        String partitionKey = "hazelcast";
        ISemaphore semaphore = instance.getSemaphore("s@" + partitionKey);
        semaphore.release();
        ContainsSemaphoreTask task = new ContainsSemaphoreTask(semaphore.getName());
        Future<Boolean> f = executorServices.submitToKeyOwner(task, semaphore.getPartitionKey());
        assertTrue(f.get());
    }

    private static class ContainsSemaphoreTask implements Callable<Boolean>, HazelcastInstanceAware, Serializable {
        private transient HazelcastInstance hz;
        private final String semaphoreName;

        private ContainsSemaphoreTask(String semaphoreName) {
            this.semaphoreName = semaphoreName;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hz = hazelcastInstance;
        }

        @Override
        public Boolean call() {
            NodeEngineImpl nodeEngine = TestUtil.getNode(hz).nodeEngine;
            SemaphoreService service = nodeEngine.getService(SemaphoreService.SERVICE_NAME);
            return service.containsSemaphore(semaphoreName);
        }
    }

    @Test
    public void testObjectWithPartitionKeyAndMap() throws Exception {
        HazelcastInstance instance = instances[0];
        IExecutorService executorServices = instance.getExecutorService("executor");
        String partitionKey = "hazelcast";
        String mapKey = "key@" + partitionKey;
        IMap map = instance.getMap("map");

        map.put(mapKey, "foobar");

        ISemaphore semaphore = instance.getSemaphore("s@" + partitionKey);
        semaphore.release();

        ContainsSemaphoreAndMapEntryTask task = new ContainsSemaphoreAndMapEntryTask(semaphore.getName(), mapKey);
        Map<Member, Future<Boolean>> futures = executorServices.submitToAllMembers(task);

        int count = 0;
        for (Future<Boolean> f : futures.values()) {
            count += f.get() ? 1 : 0;
        }

        assertEquals(1, count);
    }

    private static class ContainsSemaphoreAndMapEntryTask implements Callable<Boolean>, HazelcastInstanceAware, Serializable {
        private final String mapKey;
        private transient HazelcastInstance hz;
        private final String semaphoreName;

        private ContainsSemaphoreAndMapEntryTask(String semaphoreName, String mapKey) {
            this.semaphoreName = semaphoreName;
            this.mapKey = mapKey;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hz = hazelcastInstance;
        }

        @Override
        public Boolean call() {
            NodeEngineImpl nodeEngine = TestUtil.getNode(hz).nodeEngine;
            SemaphoreService service = nodeEngine.getService(SemaphoreService.SERVICE_NAME);

            IMap map = hz.getMap("map");
            if (map.localKeySet().contains(mapKey)) {
                return service.containsSemaphore(semaphoreName);
            } else {
                return false;
            }
        }
    }
}
