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

package com.hazelcast.internal.management;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.json.JsonObject;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class TimedMemberStateTest extends HazelcastTestSupport {

    private TimedMemberState timedMemberState;
    private HazelcastInstance hz;

    @Before
    public void setUp() {
        hz = createHazelcastInstance();
        TimedMemberStateFactory timedMemberStateFactory = new TimedMemberStateFactory(getHazelcastInstanceImpl(hz));

        timedMemberState = timedMemberStateFactory.createTimedMemberState();
        timedMemberState.setClusterName("ClusterName");
        timedMemberState.setTime(1827731);
        timedMemberState.setSslEnabled(true);
        timedMemberState.setLite(true);
        timedMemberState.setScriptingEnabled(false);
    }

    @Test
    public void testClone() throws CloneNotSupportedException {
        TimedMemberState cloned = timedMemberState.clone();

        assertNotNull(cloned);
        assertEquals("ClusterName", cloned.getClusterName());
        assertEquals(1827731, cloned.getTime());
        assertNotNull(cloned.getMemberState());
        assertTrue(cloned.isSslEnabled());
        assertTrue(cloned.isLite());
        assertFalse(cloned.isScriptingEnabled());
        assertNotNull(cloned.toString());
    }

    @Test
    public void testSerialization() {
        JsonObject serialized = timedMemberState.toJson();
        TimedMemberState deserialized = new TimedMemberState();
        deserialized.fromJson(serialized);

        assertNotNull(deserialized);
        assertEquals("ClusterName", deserialized.getClusterName());
        assertEquals(1827731, deserialized.getTime());
        assertNotNull(deserialized.getMemberState());
        assertTrue(deserialized.isSslEnabled());
        assertTrue(deserialized.isLite());
        assertFalse(deserialized.isScriptingEnabled());
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
