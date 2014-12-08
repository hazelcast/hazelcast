package com.hazelcast.security.permission;

import com.hazelcast.collection.list.ListService;
import com.hazelcast.collection.set.SetService;
import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.concurrent.atomicreference.AtomicReferenceService;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.concurrent.idgen.IdGeneratorService;
import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.semaphore.SemaphoreService;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.queue.impl.QueueService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.impl.TopicService;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.security.Permission;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ActionConstantsTest {

    @Test(expected = IllegalArgumentException.class)
    public void getPermission_whenNonExistingService() {
        ActionConstants.getPermission("foo", "idon'texist");
    }

    @Test
    public void getPermission_Map() {
        Permission permission = ActionConstants.getPermission("foo", MapService.SERVICE_NAME);

        assertNotNull(permission);
        assertTrue(permission instanceof MapPermission);
    }

    @Test
    public void getPermission_MultiMap() {
        Permission permission = ActionConstants.getPermission("foo", MultiMapService.SERVICE_NAME);

        assertNotNull(permission);
        assertTrue(permission instanceof MultiMapPermission);
    }

    @Test
    public void getPermission_List() {
        Permission permission = ActionConstants.getPermission("foo", ListService.SERVICE_NAME);

        assertNotNull(permission);
        assertTrue(permission instanceof ListPermission);
    }


    @Test
    public void getPermission_Set() {
        Permission permission = ActionConstants.getPermission("foo", SetService.SERVICE_NAME);

        assertNotNull(permission);
        assertTrue(permission instanceof SetPermission);
    }

    @Test
    public void getPermission_AtomicLong() {
        Permission permission = ActionConstants.getPermission("foo", AtomicLongService.SERVICE_NAME);

        assertNotNull(permission);
        assertTrue(permission instanceof AtomicLongPermission);
    }

    @Test
    public void getPermission_Semaphore() {
        Permission permission = ActionConstants.getPermission("foo", SemaphoreService.SERVICE_NAME);

        assertNotNull(permission);
        assertTrue(permission instanceof SemaphorePermission);
    }

    @Test
    public void getPermission_Topic() {
        Permission permission = ActionConstants.getPermission("foo", TopicService.SERVICE_NAME);

        assertNotNull(permission);
        assertTrue(permission instanceof TopicPermission);
    }

    @Test
    public void getPermission_Lock() {
        Permission permission = ActionConstants.getPermission("foo", LockService.SERVICE_NAME);

        assertNotNull(permission);
        assertTrue(permission instanceof LockPermission);
    }

    @Test
    public void getPermission_DistributedExecutor() {
        Permission permission = ActionConstants.getPermission("foo", DistributedExecutorService.SERVICE_NAME);

        assertNotNull(permission);
        assertTrue(permission instanceof ExecutorServicePermission);
    }

    @Test
    public void getPermission_IdGenerator() {
        Permission permission = ActionConstants.getPermission("foo", IdGeneratorService.SERVICE_NAME);

        assertNotNull(permission);
        assertTrue(permission instanceof AtomicLongPermission);
    }

    @Test
    public void getPermission_MapReduce() {
        Permission permission = ActionConstants.getPermission("foo", MapReduceService.SERVICE_NAME);

        assertNotNull(permission);
        assertTrue(permission instanceof MapReducePermission);
    }

    @Test
    public void getPermission_ReplicatedMap() {
        Permission permission = ActionConstants.getPermission("foo", ReplicatedMapService.SERVICE_NAME);

        assertNotNull(permission);
        assertTrue(permission instanceof ReplicatedMapPermission);
    }

    @Test
    public void getPermission_AtomicReference() {
        Permission permission = ActionConstants.getPermission("foo", AtomicReferenceService.SERVICE_NAME);

        assertNotNull(permission);
        assertTrue(permission instanceof AtomicReferencePermission);
    }

    @Test
    public void getPermission_CountdownLatch() {
        Permission permission = ActionConstants.getPermission("foo", CountDownLatchService.SERVICE_NAME);

        assertNotNull(permission);
        assertTrue(permission instanceof CountDownLatchPermission);
    }

    @Test
    public void getPermission_Queue() {
        Permission permission = ActionConstants.getPermission("foo", QueueService.SERVICE_NAME);

        assertNotNull(permission);
        assertTrue(permission instanceof QueuePermission);
    }
}
