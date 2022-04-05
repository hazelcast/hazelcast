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

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.TestPartitionUtils;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(SlowTest.class)
public class PartitionLostListenerStressTest extends AbstractPartitionLostListenerTest {

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

    protected int getNodeCount() {
        return 5;
    }

    protected int getMapEntryCount() {
        return 5000;
    }

    @Test
    public void testPartitionLostListener() {
        List<HazelcastInstance> instances = getCreatedInstancesShuffledAfterWarmedUp();
        List<HazelcastInstance> survivingInstances = new ArrayList<HazelcastInstance>(instances);
        List<HazelcastInstance> terminatingInstances = survivingInstances.subList(0, numberOfNodesToCrash);
        survivingInstances = survivingInstances.subList(numberOfNodesToCrash, instances.size());

        if (withData) {
            populateMaps(survivingInstances.get(0));
        }

        final String log = "Surviving: " + survivingInstances + " Terminating: " + terminatingInstances;
        final EventCollectingPartitionLostListener listener = registerPartitionLostListener(survivingInstances.get(0));
        final Map<Integer, Integer> survivingPartitions = getMinReplicaIndicesByPartitionId(survivingInstances);
        final Map<Integer, List<Address>> partitionTables = TestPartitionUtils.getAllReplicaAddresses(survivingInstances);

        stopInstances(terminatingInstances, nodeLeaveType);
        waitAllForSafeStateAndDumpPartitionServiceOnFailure(survivingInstances, 300);

        if (shouldExpectPartitionLostEvents) {
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() {
                    assertLostPartitions(log, listener, survivingPartitions, partitionTables);
                }
            });
        } else {
            assertTrueAllTheTime(new AssertTask() {
                @Override
                public void run() {
                    assertTrue(listener.getEvents().isEmpty());
                }
            }, 1);
        }
    }

    @Test
    public void test_partitionLostListenerNotInvoked_whenNewNodesJoin() {
        HazelcastInstance master = createInstances(1).get(0);
        EventCollectingPartitionLostListener listener = registerPartitionLostListener(master);
        List<HazelcastInstance> others = createInstances(getNodeCount() - 1);

        waitAllForSafeState(singletonList(master));
        waitAllForSafeState(others);

        assertTrue("No invocation to PartitionLostListener when new nodes join to cluster", listener.getEvents().isEmpty());
    }

    private void assertLostPartitions(String log, EventCollectingPartitionLostListener listener,
                                      Map<Integer, Integer> survivingPartitions, Map<Integer, List<Address>> partitionTables) {
        List<PartitionLostEvent> failedPartitions = listener.getEvents();

        assertFalse(survivingPartitions.isEmpty());

        for (PartitionLostEvent event : failedPartitions) {
            int failedPartitionId = event.getPartitionId();
            int lostReplicaIndex = event.getLostBackupCount();
            int survivingReplicaIndex = survivingPartitions.get(failedPartitionId);

            String message = log + ", Event: " + event.toString() + " SurvivingReplicaIndex: " + survivingReplicaIndex
                    + " PartitionTable: " + partitionTables.get(failedPartitionId);

            assertTrue(message, survivingReplicaIndex > 0);
            assertTrue(message, lostReplicaIndex >= 0 && lostReplicaIndex < survivingReplicaIndex);
        }
    }

    private EventCollectingPartitionLostListener registerPartitionLostListener(HazelcastInstance instance) {
        EventCollectingPartitionLostListener listener = new EventCollectingPartitionLostListener();
        instance.getPartitionService().addPartitionLostListener(listener);
        return listener;
    }

    public static class EventCollectingPartitionLostListener implements PartitionLostListener {

        private List<PartitionLostEvent> lostPartitions = new ArrayList<PartitionLostEvent>();

        @Override
        public synchronized void partitionLost(PartitionLostEvent event) {
            lostPartitions.add(event);
        }

        public synchronized List<PartitionLostEvent> getEvents() {
            return new ArrayList<PartitionLostEvent>(lostPartitions);
        }

        public synchronized void clear() {
            lostPartitions.clear();
        }
    }
}
