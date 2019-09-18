/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.cp.internal.datastructures.unsafe.atomiclong.AtomicLongService;
import com.hazelcast.cp.internal.datastructures.unsafe.countdownlatch.CountDownLatchService;
import com.hazelcast.cp.internal.datastructures.unsafe.lock.InternalLockNamespace;
import com.hazelcast.cp.internal.datastructures.unsafe.lock.LockServiceImpl;
import com.hazelcast.cp.internal.datastructures.unsafe.lock.LockStore;
import com.hazelcast.cp.internal.datastructures.unsafe.semaphore.SemaphoreService;
import com.hazelcast.config.Config;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.collection.IList;
import com.hazelcast.cp.lock.ILock;
import com.hazelcast.map.IMap;
import com.hazelcast.collection.IQueue;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.collection.ISet;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.cluster.Member;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.strategy.StringAndPartitionAwarePartitioningStrategy;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.internal.services.ObjectNamespace;
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
    public void testLock() {
        String partitionKey = "hazelcast";
        HazelcastInstance hz = getHazelcastInstance(partitionKey);

        ILock lock = hz.getLock("lock@" + partitionKey);
        lock.lock();
        assertEquals("lock@" + partitionKey, lock.getName());
        assertEquals(partitionKey, lock.getPartitionKey());

        Node node = getNode(hz);
        LockServiceImpl lockService = node.nodeEngine.getService(LockServiceImpl.SERVICE_NAME);

        Partition partition = instances[0].getPartitionService().getPartition(partitionKey);
        LockStore lockStore = lockService.getLockStore(partition.getPartitionId(), new InternalLockNamespace(lock.getName()));
        Data key = node.getSerializationService().toData(lock.getName(), StringPartitioningStrategy.INSTANCE);
        assertTrue(lockStore.isLocked(key));
    }

    @Test
    public void testSemaphore() {
        String partitionKey = "hazelcast";
        HazelcastInstance hz = getHazelcastInstance(partitionKey);

        ISemaphore semaphore = hz.getSemaphore("semaphore@" + partitionKey);
        semaphore.release();
        assertEquals("semaphore@" + partitionKey, semaphore.getName());
        assertEquals(partitionKey, semaphore.getPartitionKey());

        SemaphoreService service = getNodeEngine(hz).getService(SemaphoreService.SERVICE_NAME);
        assertTrue(service.containsSemaphore(semaphore.getName()));
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
    public void testIdGenerator() {
        String partitionKey = "hazelcast";
        HazelcastInstance hz = getHazelcastInstance(partitionKey);

        IdGenerator idGenerator = hz.getIdGenerator("idgenerator@" + partitionKey);
        idGenerator.newId();
        assertEquals("idgenerator@" + partitionKey, idGenerator.getName());
        assertEquals(partitionKey, idGenerator.getPartitionKey());

        AtomicLongService service = getNodeEngine(hz).getService(AtomicLongService.SERVICE_NAME);
        assertTrue(service.containsAtomicLong("hz:atomic:idGenerator:" + idGenerator.getName()));
    }

    @Test
    public void testAtomicLong() {
        String partitionKey = "hazelcast";
        HazelcastInstance hz = getHazelcastInstance(partitionKey);

        IAtomicLong atomicLong = hz.getAtomicLong("atomiclong@" + partitionKey);
        atomicLong.incrementAndGet();
        assertEquals("atomiclong@" + partitionKey, atomicLong.getName());
        assertEquals(partitionKey, atomicLong.getPartitionKey());

        AtomicLongService service = getNodeEngine(hz).getService(AtomicLongService.SERVICE_NAME);
        assertTrue(service.containsAtomicLong(atomicLong.getName()));
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
    public void testCountDownLatch() {
        String partitionKey = "hazelcast";
        HazelcastInstance hz = getHazelcastInstance(partitionKey);

        ICountDownLatch countDownLatch = hz.getCountDownLatch("countDownLatch@" + partitionKey);
        countDownLatch.trySetCount(1);
        assertEquals("countDownLatch@" + partitionKey, countDownLatch.getName());
        assertEquals(partitionKey, countDownLatch.getPartitionKey());

        CountDownLatchService service = getNodeEngine(hz).getService(CountDownLatchService.SERVICE_NAME);
        assertTrue(service.containsLatch(countDownLatch.getName()));
    }

    @Test
    public void testObjectWithPartitionKeyAndTask() throws Exception {
        HazelcastInstance instance = instances[0];
        IExecutorService executorServices = instance.getExecutorService("executor");
        String partitionKey = "hazelcast";
        ISemaphore semaphore = instance.getSemaphore("foobar@" + partitionKey);
        semaphore.release();
        ContainsSemaphoreTask task = new ContainsSemaphoreTask(semaphore.getName());
        Future<Boolean> future = executorServices.submitToKeyOwner(task, semaphore.getPartitionKey());
        assertTrue(future.get());
    }

    private static class ContainsSemaphoreTask implements Callable<Boolean>, HazelcastInstanceAware, Serializable {

        private final String semaphoreName;

        private transient HazelcastInstance hz;

        private ContainsSemaphoreTask(String semaphoreName) {
            this.semaphoreName = semaphoreName;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hz = hazelcastInstance;
        }

        @Override
        public Boolean call() {
            NodeEngineImpl nodeEngine = getNode(hz).nodeEngine;
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
        IMap<String, String> map = instance.getMap("map");

        map.put(mapKey, "foobar");

        ISemaphore semaphore = instance.getSemaphore("s@" + partitionKey);
        semaphore.release();

        ContainsSemaphoreAndMapEntryTask task = new ContainsSemaphoreAndMapEntryTask(semaphore.getName(), mapKey);
        Map<Member, Future<Boolean>> futures = executorServices.submitToAllMembers(task);

        int count = 0;
        for (Future<Boolean> future : futures.values()) {
            count += future.get() ? 1 : 0;
        }

        assertEquals(1, count);
    }

    private static class ContainsSemaphoreAndMapEntryTask implements Callable<Boolean>, HazelcastInstanceAware, Serializable {

        private final String semaphoreName;
        private final String mapKey;

        private transient HazelcastInstance hz;

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
            NodeEngineImpl nodeEngine = getNode(hz).nodeEngine;
            SemaphoreService service = nodeEngine.getService(SemaphoreService.SERVICE_NAME);

            IMap map = hz.getMap("map");
            return map.localKeySet().contains(mapKey) && service.containsSemaphore(semaphoreName);
        }
    }
}
