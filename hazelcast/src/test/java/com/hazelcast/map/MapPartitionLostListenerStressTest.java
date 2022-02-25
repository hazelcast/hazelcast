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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.partition.AbstractPartitionLostListenerTest;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(SlowTest.class)
public class MapPartitionLostListenerStressTest extends AbstractPartitionLostListenerTest {

    @Parameters(name = "numberOfNodesToCrash:{0},withData:{1},nodeLeaveType:{2},shouldExpectPartitionLostEvents:{3}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {1, true, NodeLeaveType.SHUTDOWN, false},
                {1, true, NodeLeaveType.TERMINATE, true},
                {1, false, NodeLeaveType.SHUTDOWN, false},
                {1, false, NodeLeaveType.TERMINATE, true},
                {2, true, NodeLeaveType.SHUTDOWN, false},
                {2, true, NodeLeaveType.TERMINATE, true},
                {2, false, NodeLeaveType.SHUTDOWN, false},
                {2, false, NodeLeaveType.TERMINATE, true},
                {3, true, NodeLeaveType.SHUTDOWN, false},
                {3, true, NodeLeaveType.TERMINATE, true},
                {3, false, NodeLeaveType.SHUTDOWN, false},
                {3, false, NodeLeaveType.TERMINATE, true},
        });
    }

    @Parameter(0)
    public int numberOfNodesToCrash;

    @Parameter(1)
    public boolean withData;

    @Parameter(2)
    public NodeLeaveType nodeLeaveType;

    @Parameter(3)
    public boolean shouldExpectPartitionLostEvents;

    @Override
    protected int getNodeCount() {
        return 5;
    }

    @Override
    protected int getMapEntryCount() {
        return 5000;
    }

    @Test
    public void testMapPartitionLostListener() throws InterruptedException {
        List<HazelcastInstance> instances = getCreatedInstancesShuffledAfterWarmedUp();

        List<HazelcastInstance> survivingInstances = new ArrayList<HazelcastInstance>(instances);
        List<HazelcastInstance> terminatingInstances = survivingInstances.subList(0, numberOfNodesToCrash);
        survivingInstances = survivingInstances.subList(numberOfNodesToCrash, instances.size());

        List<TestEventCollectingMapPartitionLostListener> listeners = registerListeners(survivingInstances.get(0));

        if (withData) {
            populateMaps(survivingInstances.get(0));
        }

        String log = "Surviving: " + survivingInstances + " Terminating: " + terminatingInstances;
        Map<Integer, Integer> survivingPartitions = getMinReplicaIndicesByPartitionId(survivingInstances);

        stopInstances(terminatingInstances, nodeLeaveType);
        waitAllForSafeStateAndDumpPartitionServiceOnFailure(survivingInstances, 300);

        if (shouldExpectPartitionLostEvents) {
            for (int i = 0; i < getNodeCount(); i++) {
                assertListenerInvocationsEventually(log, i, numberOfNodesToCrash, listeners.get(i), survivingPartitions);
            }
        } else {
            for (final TestEventCollectingMapPartitionLostListener listener : listeners) {
                assertTrueAllTheTime(new AssertTask() {
                    @Override
                    public void run()
                            throws Exception {
                        assertTrue(listener.getEvents().isEmpty());
                    }
                }, 1);
            }
        }
    }

    private List<TestEventCollectingMapPartitionLostListener> registerListeners(HazelcastInstance instance) {
        List<TestEventCollectingMapPartitionLostListener> listeners
                = new ArrayList<TestEventCollectingMapPartitionLostListener>();
        for (int i = 0; i < getNodeCount(); i++) {
            TestEventCollectingMapPartitionLostListener listener = new TestEventCollectingMapPartitionLostListener(i);
            instance.getMap(getIthMapName(i)).addPartitionLostListener(listener);
            listeners.add(listener);
        }
        return listeners;
    }

    private static void assertLostPartitions(String log, TestEventCollectingMapPartitionLostListener listener,
                                             Map<Integer, Integer> survivingPartitions) {
        List<MapPartitionLostEvent> events = listener.getEvents();
        assertFalse(survivingPartitions.isEmpty());

        for (MapPartitionLostEvent event : events) {
            int failedPartitionId = event.getPartitionId();
            Integer survivingReplicaIndex = survivingPartitions.get(failedPartitionId);
            if (survivingReplicaIndex != null) {
                String message = log + ", PartitionId: " + failedPartitionId + " SurvivingReplicaIndex: " + survivingReplicaIndex
                        + " Event: " + event.toString();
                assertTrue(message, survivingReplicaIndex > listener.getBackupCount());
            }
        }
    }

    private static void assertListenerInvocationsEventually(final String log, final int index, final int numberOfNodesToCrash,
                                                            final TestEventCollectingMapPartitionLostListener listener,
                                                            final Map<Integer, Integer> survivingPartitions) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                if (index < numberOfNodesToCrash) {
                    assertLostPartitions(log, listener, survivingPartitions);
                } else {
                    String message = log + " listener-" + index + " should not be invoked!";
                    assertTrue(message, listener.getEvents().isEmpty());
                }
            }
        });
    }
}
