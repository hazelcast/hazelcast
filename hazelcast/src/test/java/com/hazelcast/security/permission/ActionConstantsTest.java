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

package com.hazelcast.security.permission;

import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.cp.internal.datastructures.atomiclong.AtomicLongService;
import com.hazelcast.cp.internal.datastructures.atomicref.AtomicRefService;
import com.hazelcast.cp.internal.datastructures.countdownlatch.CountDownLatchService;
import com.hazelcast.cp.internal.datastructures.semaphore.SemaphoreService;
import com.hazelcast.internal.locksupport.LockSupportService;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.flakeidgen.impl.FlakeIdGeneratorService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.multimap.impl.MultiMapService;
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
    public void getPermission_Cache() {
        Permission permission = ActionConstants.getPermission("foo", ICacheService.SERVICE_NAME);

        assertNotNull(permission);
        assertTrue(permission instanceof CachePermission);
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
        Permission permission = ActionConstants.getPermission("foo", LockSupportService.SERVICE_NAME);

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
    public void getPermission_FlakeIdGenerator() {
        Permission permission = ActionConstants.getPermission("foo", FlakeIdGeneratorService.SERVICE_NAME);

        assertNotNull(permission);
        assertTrue(permission instanceof FlakeIdGeneratorPermission);
    }

    @Test
    public void getPermission_ReplicatedMap() {
        Permission permission = ActionConstants.getPermission("foo", ReplicatedMapService.SERVICE_NAME);

        assertNotNull(permission);
        assertTrue(permission instanceof ReplicatedMapPermission);
    }

    @Test
    public void getPermission_AtomicReference() {
        Permission permission = ActionConstants.getPermission("foo", AtomicRefService.SERVICE_NAME);

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
