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
import com.hazelcast.collection.set.ObjectSetProxy;
import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.concurrent.lock.InternalLockNamespace;
import com.hazelcast.concurrent.lock.LockServiceImpl;
import com.hazelcast.concurrent.lock.LockStore;
import com.hazelcast.concurrent.semaphore.SemaphoreService;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.instance.Node;
import com.hazelcast.queue.QueueService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author mdogan 8/21/13
 */

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class PartitionControlledIdTest extends HazelcastTestSupport {

    @Test
    public void test() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        Config config = new Config();
        HazelcastInstance[] instances = factory.newInstances(config);
        warmUpPartitions(instances);

        String partitionKey = "someKey";
        String id = "foo@" + partitionKey;

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

        hz.getQueue(id).offer(1);
        hz.getSemaphore(id).release(1);
        hz.getCountDownLatch(id).trySetCount(5);
        hz.getAtomicLong(id).set(111);
        hz.getList(id).add(1);
        hz.getSet(id).add(2);
        hz.getLock(id).lock();

        Node node = getNode(hz);
        NodeEngineImpl nodeEngine = node.nodeEngine;

        QueueService queueService = nodeEngine.getService(QueueService.SERVICE_NAME);
        assertTrue(queueService.containsQueue(id));

        SemaphoreService semaphoreService = nodeEngine.getService(SemaphoreService.SERVICE_NAME);
        assertTrue(semaphoreService.containsSemaphore(id));

        CountDownLatchService latchService = nodeEngine.getService(CountDownLatchService.SERVICE_NAME);
        assertTrue(latchService.containsLatch(id));

        AtomicLongService atomicLongService = nodeEngine.getService(AtomicLongService.SERVICE_NAME);
        assertTrue(atomicLongService.containsAtomicLong(id));

        CollectionService collectionService = nodeEngine.getService(CollectionService.SERVICE_NAME);
        CollectionPartitionContainer cpc = collectionService.getPartitionContainer(partition.getPartitionId());
        assertTrue(cpc.containsCollection(new CollectionProxyId(ObjectListProxy.COLLECTION_LIST_NAME, id, CollectionProxyType.LIST)));
        assertTrue(cpc.containsCollection(new CollectionProxyId(ObjectSetProxy.COLLECTION_SET_NAME, id, CollectionProxyType.SET)));

        LockServiceImpl lockService = nodeEngine.getService(LockServiceImpl.SERVICE_NAME);
        LockStore lockStore = lockService.getLockStore(partition.getPartitionId(), new InternalLockNamespace());
        assertTrue(lockStore.isLocked(node.getSerializationService().toData(id)));
    }
}
