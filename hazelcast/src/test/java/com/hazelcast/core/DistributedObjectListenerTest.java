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

package com.hazelcast.core;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.internal.util.ThreadLocalRandomProvider;
import com.hazelcast.map.IMap;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class DistributedObjectListenerTest extends HazelcastTestSupport {

    protected final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory(3);
    HazelcastInstance[] servers;

    @Before
    public void setUp() {
        servers = hazelcastFactory.newInstances();
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    protected HazelcastInstance newInstance() {
        return hazelcastFactory.newHazelcastInstance();
    }

    protected HazelcastInstance getRandomServer() {
        Random random = ThreadLocalRandomProvider.get();
        return servers[random.nextInt(servers.length)];
    }

    @Test
    public void testGetDistributedObjectsAfterRemove_FromNode() {
        HazelcastInstance server = getRandomServer();
        IMap firstMap = server.getMap("firstMap");
        server.getMap("secondMap");

        HazelcastInstance instance = newInstance();
        assertEquals(2, instance.getDistributedObjects().size());

        firstMap.destroy();

        assertEquals(1, instance.getDistributedObjects().size());

    }

    @Test
    public void testGetDistributedObjectsAfterDestroy_fromInstance() {
        HazelcastInstance instance1 = newInstance();
        IMap<Object, Object> firstMap = instance1.getMap("firstMap");
        instance1.getMap("secondMap");

        HazelcastInstance instance2 = newInstance();
        assertEquals(2, instance1.getDistributedObjects().size());
        assertEquals(2, instance2.getDistributedObjects().size());

        firstMap.destroy();

        assertEquals(1, instance1.getDistributedObjects().size());
        assertEquals(1, instance2.getDistributedObjects().size());
    }

    @Test
    public void getDistributedObjects_ShouldNotRecreateProxy_AfterDestroy() {
        HazelcastInstance member = getRandomServer();
        HazelcastInstance client = newInstance();
        Future destroyProxyFuture = spawn(() -> {
            for (int i = 0; i < 1000; i++) {
                IMap<Object, Object> map = member.getMap("map-" + i);
                map.destroy();
            }
        });
        while (!destroyProxyFuture.isDone()) {
            client.getDistributedObjects();
        }
        assertEquals(0, client.getDistributedObjects().size());
    }

    @Test
    public void testDestroyEventReceived_WhenDestroyedFromTheSameInstance() {
        final HazelcastInstance instance = newInstance();
        EventCountListener listener = new EventCountListener();
        instance.addDistributedObjectListener(listener);
        IMap<Object, Object> map = instance.getMap(randomString());
        map.destroy();
        AssertTask task = new AssertTask() {
            @Override
            public void run() throws Exception {
                Assert.assertEquals(1, listener.createdCount.get());
                Assert.assertEquals(1, listener.destroyedCount.get());
                Collection<DistributedObject> distributedObjects = instance.getDistributedObjects();
                Assert.assertTrue(distributedObjects.isEmpty());
            }
        };
        assertTrueEventually(task, 5);
        assertTrueAllTheTime(task, 3);
    }

    @Test
    public void testDestroyEventReceived_WhenDestroyedFromDifferentInstance() {
        final HazelcastInstance instance1 = newInstance();
        final HazelcastInstance instance2 = newInstance();
        EventCountListener listener = new EventCountListener();
        instance1.addDistributedObjectListener(listener);
        String randomMapName = randomMapName();
        IMap<Object, Object> map1 = instance1.getMap(randomMapName);
        IMap<Object, Object> map2 = instance2.getMap(randomMapName);
        map2.destroy();
        AssertTask task = new AssertTask() {
            @Override
            public void run() throws Exception {
                Assert.assertEquals(1, listener.createdCount.get());
                Assert.assertEquals(1, listener.destroyedCount.get());
                Collection<DistributedObject> distributedObjects = instance1.getDistributedObjects();
                Assert.assertTrue(distributedObjects.isEmpty());
            }
        };
        assertTrueEventually(task, 5);
        assertTrueAllTheTime(task, 3);
    }

    @Test
    public void testDestroyEventReceived_WhenDestroyedByServer() {
        final HazelcastInstance instance = newInstance();
        final HazelcastInstance server = getRandomServer();
        EventCountListener listener = new EventCountListener();
        instance.addDistributedObjectListener(listener);
        String mapName = randomMapName();
        IMap<Object, Object> map1 = instance.getMap(mapName);
        IMap<Object, Object> map2 = server.getMap(mapName);
        map2.destroy();
        AssertTask task = new AssertTask() {
            @Override
            public void run() throws Exception {
                Assert.assertEquals(1, listener.createdCount.get());
                Assert.assertEquals(1, listener.destroyedCount.get());
                Collection<DistributedObject> distributedObjects = instance.getDistributedObjects();
                Assert.assertTrue(distributedObjects.isEmpty());
            }
        };
        assertTrueEventually(task, 5);
        assertTrueAllTheTime(task, 3);
    }

    public static class EventCountListener implements DistributedObjectListener {

        public AtomicInteger createdCount = new AtomicInteger();
        public AtomicInteger destroyedCount = new AtomicInteger();

        public void distributedObjectCreated(DistributedObjectEvent event) {
            createdCount.incrementAndGet();
        }

        public void distributedObjectDestroyed(DistributedObjectEvent event) {
            destroyedCount.incrementAndGet();
        }
    }
}
