package com.hazelcast.monitor;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.management.TimedMemberStateFactory;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Set;

import static com.hazelcast.instance.TestUtil.getHazelcastInstanceImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class TimedMemberStateIntegrationTest extends HazelcastTestSupport{

    private HazelcastInstance hz;
    private TimedMemberStateFactory factory;

    @Before
    public void setUp() {
        hz = createHazelcastInstance();
        factory = new TimedMemberStateFactory(getHazelcastInstanceImpl(hz));
    }

    @Test
    public void testServices() {
        hz.getMap("trial").put(1, 1);
        hz.getMultiMap("trial").put(2, 2);
        hz.getQueue("trial").offer(3);
        hz.getTopic("trial").publish("Hello");
        hz.getReplicatedMap("trial").put(3, 3);
        hz.getExecutorService("trial");

        TimedMemberState timedMemberState = factory.createTimedMemberState();
        Set<String> instanceNames = timedMemberState.getInstanceNames();

        assertEquals("dev", timedMemberState.clusterName);
        assertTrue(instanceNames.contains("c:trial"));
        assertTrue(instanceNames.contains("m:trial"));
        assertTrue(instanceNames.contains("q:trial"));
        assertTrue(instanceNames.contains("t:trial"));
        assertTrue(instanceNames.contains("r:trial"));
        assertTrue(instanceNames.contains("e:trial"));
    }
}
