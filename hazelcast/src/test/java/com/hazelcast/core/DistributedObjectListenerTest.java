/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
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
        checkTheNumberOfObjectsInClusterIsEventuallyAsExpected(2);
        assertEquals(2, instance.getDistributedObjects().size());

        firstMap.destroy();

        checkTheNumberOfObjectsInClusterIsEventuallyAsExpected(1);
        assertEquals(1, instance.getDistributedObjects().size());
    }

    @Test
    public void testGetDistributedObjectsAfterDestroy_fromInstance() {
        HazelcastInstance instance1 = newInstance();
        IMap<Object, Object> firstMap = instance1.getMap("firstMap");
        instance1.getMap("secondMap");

        HazelcastInstance instance2 = newInstance();
        checkTheNumberOfObjectsInClusterIsEventuallyAsExpected(2);
        assertEquals(2, instance1.getDistributedObjects().size());
        assertEquals(2, instance2.getDistributedObjects().size());

        firstMap.destroy();

        checkTheNumberOfObjectsInClusterIsEventuallyAsExpected(1);
        assertEquals(1, instance1.getDistributedObjects().size());
        assertEquals(1, instance2.getDistributedObjects().size());
    }

    @Test
    public void getDistributedObjects_ShouldNotRecreateProxy_AfterDestroy() {
        HazelcastInstance member = getRandomServer();
        HazelcastInstance instance = newInstance();
        Future destroyProxyFuture = spawn(() -> {
            for (int i = 0; i < 1000; i++) {
                IMap<Object, Object> map = member.getMap("map-" + i);
                map.destroy();
            }
        });
        while (!destroyProxyFuture.isDone()) {
            instance.getDistributedObjects();
        }
        checkTheNumberOfObjectsInClusterIsEventuallyAsExpected(0);
        assertEquals(0, instance.getDistributedObjects().size());
    }

    @Test
    public void testDestroyEventReceived_WhenDestroyedFromTheSameInstance() {
        final HazelcastInstance instance = newInstance();
        String mapName = randomMapName();
        EventCountListener listener = new EventCountListener(mapName);
        instance.addDistributedObjectListener(listener);
        IMap<Object, Object> map = instance.getMap(mapName);

        // TODO: This line is not needed when the create destroy order is guaranteed.
        // The issue: https://github.com/hazelcast/hazelcast/issues/16374
        checkTheNumberOfObjectsInClusterIsEventuallyAsExpected(1);

        map.destroy();

        verifyDestroy(instance, instance, listener);
    }

    @Test
    public void testDestroyEventReceived_WhenDestroyedFromDifferentInstance() {
        final HazelcastInstance instance1 = newInstance();
        final HazelcastInstance instance2 = newInstance();
        String mapName = randomMapName();
        EventCountListener listener = new EventCountListener(mapName);
        instance1.addDistributedObjectListener(listener);
        instance1.getMap(mapName);

        // TODO: This line is not needed when the create destroy order is guaranteed.
        // The issue: https://github.com/hazelcast/hazelcast/issues/16374
        checkTheNumberOfObjectsInClusterIsEventuallyAsExpected(1);

        IMap<Object, Object> map2 = instance2.getMap(mapName);
        map2.destroy();

        verifyDestroy(instance1, instance2, listener);
    }

    @Test
    public void testDestroyEventReceived_WhenDestroyedByServer() {
        final HazelcastInstance instance = newInstance();
        final HazelcastInstance server = getRandomServer();
        String mapName = randomMapName();
        EventCountListener listener = new EventCountListener(mapName);
        instance.addDistributedObjectListener(listener);
        instance.getMap(mapName);

        // TODO: This line is not needed when the create destroy order is guaranteed.
        // The issue: https://github.com/hazelcast/hazelcast/issues/16374
        checkTheNumberOfObjectsInClusterIsEventuallyAsExpected(1);

        IMap<Object, Object> map2 = server.getMap(mapName);
        map2.destroy();

        verifyDestroy(instance, server, listener);
    }

    public static class EventCountListener implements DistributedObjectListener {
        public AtomicInteger createdCount = new AtomicInteger();
        public AtomicInteger destroyedCount = new AtomicInteger();
        public List<DistributedObjectEvent> events = new CopyOnWriteArrayList();

        private final String objectName;

        public EventCountListener(String objectName) {
            this.objectName = objectName;
        }

        public void distributedObjectCreated(DistributedObjectEvent event) {
            Object objectName = event.getObjectName();
            if (objectName.equals(this.objectName)) {
                createdCount.incrementAndGet();
            }

            events.add(event);
        }

        public void distributedObjectDestroyed(DistributedObjectEvent event) {
            Object objectName = event.getObjectName();
            if (objectName.equals(this.objectName)) {
                destroyedCount.incrementAndGet();
            }

            events.add(event);
        }

        @Override
        public String toString() {
            return "EventCountListener{" + "createdCount=" + createdCount + ", destroyedCount=" + destroyedCount + ", events="
                    + events + ", objectName='" + objectName + '\'' + '}';
        }
    }

    private void verifyDestroy(HazelcastInstance listeningInstance, HazelcastInstance destroyingInstance,
                               EventCountListener listener) {
        AssertTask task = getVerifyProxyDestroyedTask(listeningInstance, destroyingInstance, listener);
        assertTrueEventually(task);
        assertTrueAllTheTime(task, 3);
    }

    private AssertTask getVerifyProxyDestroyedTask(HazelcastInstance listeningInstance, HazelcastInstance destroyingInstance,
                                                   EventCountListener listener) {
        return () -> {
            Assert.assertEquals(1, listener.destroyedCount.get());
            // Make sure that all servers deleted the proxy
            checkTheNumberOfObjectsInClusterIsEventuallyAsExpected(0);

            // if the instance is a client, this call may re-create the proxy since the client makes an invocation to a
            // random member and if the server did not delete the local proxy via receiving the destroyed event from the
            // other server yet, it may cause a problem. Therefore, we have the previous verification step to verify
            // that all servers in cluster deleted the proxy locally.
            Collection<DistributedObject> distributedObjects = listeningInstance.getDistributedObjects();
            Assert.assertTrue("Listening instance proxies are not destroyed! listeningInstance:" + listeningInstance
                            + " destroyingInstance:" + destroyingInstance + ", proxies:" + distributedObjects + ", listener:" + listener,
                    distributedObjects.isEmpty());

            distributedObjects = destroyingInstance.getDistributedObjects();
            Assert.assertTrue("Destroying instance proxies are not destroyed! listeningInstance:" + listeningInstance
                            + " destroyingInstance:" + destroyingInstance + ", proxies:" + distributedObjects + ", listener:" + listener,
                    distributedObjects.isEmpty());

            assertFalse("No event received. " + listener, listener.events.isEmpty());
            DistributedObjectEvent event = listener.events.get(listener.events.size() - 1);
            assertEquals("Last received event is not a destroy event" + listener, DistributedObjectEvent.EventType.DESTROYED,
                    event.getEventType());
            Assert.assertEquals(destroyingInstance.getLocalEndpoint().getUuid(), event.getSource());
        };
    }

    protected void checkTheNumberOfObjectsInClusterIsEventuallyAsExpected(int numberOfObjects) {
        // getDistributedObjects() call may be done against a random node when instance is client and we need to have the creation event propagated to all cluster first
        assertTrueEventually(() -> {
            hazelcastFactory.getAllHazelcastInstances().forEach(member -> {
                Collection<DistributedObject> distributedObjects = member.getDistributedObjects();
                assertEquals("Number of distributed objects is not as expected for member" + member + ", objects:"
                        + distributedObjects, numberOfObjects, distributedObjects.size());
            });
        });
    }
}
