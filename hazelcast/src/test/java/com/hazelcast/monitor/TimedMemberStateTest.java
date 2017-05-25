/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.monitor;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.management.TimedMemberStateFactory;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.instance.TestUtil.getHazelcastInstanceImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class TimedMemberStateTest extends HazelcastTestSupport {

    private TimedMemberState timedMemberState;
    private HazelcastInstance hz;

    @Before
    public void setUp() {
        Set<String> instanceNames = new HashSet<String>();
        instanceNames.add("topicStats");

        hz = createHazelcastInstance();
        TimedMemberStateFactory timedMemberStateFactory = new TimedMemberStateFactory(getHazelcastInstanceImpl(hz));

        timedMemberState = timedMemberStateFactory.createTimedMemberState();
        timedMemberState.setClusterName("ClusterName");
        timedMemberState.setTime(1827731);
        timedMemberState.setInstanceNames(instanceNames);
        timedMemberState.setSslEnabled(true);
        timedMemberState.setLite(true);
    }

    @Test
    public void testClone() throws InterruptedException, CloneNotSupportedException {
        TimedMemberState cloned = timedMemberState.clone();

        assertNotNull(cloned);
        assertEquals("ClusterName", cloned.getClusterName());
        assertEquals(1827731, cloned.getTime());
        assertNotNull(cloned.getInstanceNames());
        assertEquals(1, cloned.getInstanceNames().size());
        assertContains(cloned.getInstanceNames(), "topicStats");
        assertNotNull(cloned.getMemberState());
        assertTrue(cloned.isSslEnabled());
        assertTrue(cloned.isLite());
        assertNotNull(cloned.toString());
    }

    @Test
    public void testSerialization() throws InterruptedException, CloneNotSupportedException {
        JsonObject serialized = timedMemberState.toJson();
        TimedMemberState deserialized = new TimedMemberState();
        deserialized.fromJson(serialized);

        assertNotNull(deserialized);
        assertEquals("ClusterName", deserialized.getClusterName());
        assertEquals(1827731, deserialized.getTime());
        assertNotNull(deserialized.getInstanceNames());
        assertEquals(1, deserialized.getInstanceNames().size());
        assertContains(deserialized.getInstanceNames(), "topicStats");
        assertNotNull(deserialized.getMemberState());
        assertTrue(deserialized.isSslEnabled());
        assertTrue(deserialized.isLite());
        assertNotNull(deserialized.toString());
    }

    @Test
    public void testReplicatedMapGetStats() {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        hz.getReplicatedMap("replicatedMap");
        ReplicatedMapService replicatedMapService = nodeEngine.getService(ReplicatedMapService.SERVICE_NAME);
        assertNotNull(replicatedMapService.getStats().get("replicatedMap"));
    }
}
