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
    HazelcastInstance hz;

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
    }

    @Test
    public void testClone() throws InterruptedException, CloneNotSupportedException {
        TimedMemberState cloned = timedMemberState.clone();

        assertNotNull(cloned);
        assertEquals("ClusterName", cloned.getClusterName());
        assertEquals(1827731, cloned.getTime());
        assertNotNull(cloned.getInstanceNames());
        assertEquals(1, cloned.getInstanceNames().size());
        assertTrue(cloned.getInstanceNames().contains("topicStats"));
        assertNotNull(cloned.getMemberState());
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
        assertTrue(deserialized.getInstanceNames().contains("topicStats"));
        assertNotNull(deserialized.getMemberState());
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
