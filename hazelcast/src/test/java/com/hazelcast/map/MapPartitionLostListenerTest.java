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

package com.hazelcast.map;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.internal.partition.IPartitionLostEvent;
import com.hazelcast.internal.partition.PartitionLostEventImpl;
import com.hazelcast.map.impl.MapPartitionLostEventFilter;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.partition.AbstractPartitionLostListenerTest;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapPartitionLostListenerTest extends AbstractPartitionLostListenerTest {

    @Override
    public int getNodeCount() {
        return 2;
    }

    @Test
    public void test_partitionLostListenerInvoked() {
        List<HazelcastInstance> instances = getCreatedInstancesShuffledAfterWarmedUp(1);
        HazelcastInstance instance = instances.get(0);

        final TestEventCollectingMapPartitionLostListener listener = new TestEventCollectingMapPartitionLostListener(0);
        instance.getMap(getIthMapName(0)).addPartitionLostListener(listener);

        final IPartitionLostEvent internalEvent = new PartitionLostEventImpl(1, 0, null);

        MapService mapService = getNode(instance).getNodeEngine().getService(MapService.SERVICE_NAME);
        mapService.onPartitionLost(internalEvent);

        assertEventEventually(listener, internalEvent);
    }

    @Test
    public void test_allPartitionLostListenersInvoked() {
        List<HazelcastInstance> instances = getCreatedInstancesShuffledAfterWarmedUp(2);
        HazelcastInstance instance1 = instances.get(0);
        HazelcastInstance instance2 = instances.get(0);

        final TestEventCollectingMapPartitionLostListener listener1 = new TestEventCollectingMapPartitionLostListener(0);
        final TestEventCollectingMapPartitionLostListener listener2 = new TestEventCollectingMapPartitionLostListener(0);
        instance1.getMap(getIthMapName(0)).addPartitionLostListener(listener1);
        instance2.getMap(getIthMapName(0)).addPartitionLostListener(listener2);

        final IPartitionLostEvent internalEvent = new PartitionLostEventImpl(1, 0, null);

        MapService mapService = getNode(instance1).getNodeEngine().getService(MapService.SERVICE_NAME);
        mapService.onPartitionLost(internalEvent);

        assertEventEventually(listener1, internalEvent);
        assertEventEventually(listener2, internalEvent);
    }

    @Test
    public void test_partitionLostListenerInvoked_whenEntryListenerIsAlsoRegistered() {
        List<HazelcastInstance> instances = getCreatedInstancesShuffledAfterWarmedUp(1);
        HazelcastInstance instance = instances.get(0);

        final TestEventCollectingMapPartitionLostListener listener = new TestEventCollectingMapPartitionLostListener(0);
        instance.getMap(getIthMapName(0)).addPartitionLostListener(listener);
        instance.getMap(getIthMapName(0)).addEntryListener(mock(EntryAddedListener.class), true);

        final IPartitionLostEvent internalEvent = new PartitionLostEventImpl(1, 0, null);

        MapService mapService = getNode(instance).getNodeEngine().getService(MapService.SERVICE_NAME);
        mapService.onPartitionLost(internalEvent);

        assertEventEventually(listener, internalEvent);
    }

    @Test
    public void test_partitionLostListenerInvoked_whenNodeCrashed() {
        List<HazelcastInstance> instances = getCreatedInstancesShuffledAfterWarmedUp();

        HazelcastInstance survivingInstance = instances.get(0);
        HazelcastInstance terminatingInstance = instances.get(1);

        final TestEventCollectingMapPartitionLostListener listener = new TestEventCollectingMapPartitionLostListener(0);
        survivingInstance.getMap(getIthMapName(0)).addPartitionLostListener(listener);

        final Set<Integer> survivingPartitionIds = new HashSet<Integer>();
        Node survivingNode = getNode(survivingInstance);
        Address survivingAddress = survivingNode.getThisAddress();

        for (IPartition partition : survivingNode.getPartitionService().getPartitions()) {
            if (survivingAddress.equals(partition.getReplicaAddress(0))) {
                survivingPartitionIds.add(partition.getPartitionId());
            }
        }

        terminatingInstance.getLifecycleService().terminate();
        waitAllForSafeState(survivingInstance);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                List<MapPartitionLostEvent> events = listener.getEvents();
                assertFalse(events.isEmpty());

                for (MapPartitionLostEvent event : events) {
                    assertFalse(survivingPartitionIds.contains(event.getPartitionId()));
                }
            }
        });
    }

    @Test
    public void testMapPartitionLostEventFilter() {
        MapPartitionLostEventFilter filter = new MapPartitionLostEventFilter();
        assertEquals(new MapPartitionLostEventFilter(), filter);
        assertFalse(filter.eval(null));
    }

    private static void assertEventEventually(final TestEventCollectingMapPartitionLostListener listener,
                                              final IPartitionLostEvent internalEvent) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                List<MapPartitionLostEvent> events = listener.getEvents();
                assertEquals(1, events.size());
                MapPartitionLostEvent event = events.get(0);
                assertEquals(internalEvent.getPartitionId(), event.getPartitionId());
                assertNotNull(event.toString());
            }
        });
    }
}
