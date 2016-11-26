package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClusterStateChangeTest {

    private ClusterStateChange clusterStateChange;
    private ClusterStateChange clusterStateChangeSameAttributes;
    private ClusterStateChange clusterStateChangeOtherType;
    private ClusterStateChange clusterStateChangeOtherNewState;

    @Before
    public void setUp() {
        clusterStateChange = ClusterStateChange.from(ClusterState.ACTIVE);
        clusterStateChangeSameAttributes = ClusterStateChange.from(ClusterState.ACTIVE);
        clusterStateChangeOtherType = ClusterStateChange.from(Version.UNKNOWN);
        clusterStateChangeOtherNewState = ClusterStateChange.from(ClusterState.FROZEN);
    }

    @Test
    public void testGetType() {
        assertEquals(ClusterState.class, clusterStateChange.getType());
        assertEquals(ClusterState.class, clusterStateChangeSameAttributes.getType());
        assertEquals(Version.class, clusterStateChangeOtherType.getType());
        assertEquals(ClusterState.class, clusterStateChangeOtherNewState.getType());
    }

    @Test
    public void testGetNewState() {
        assertEquals(ClusterState.ACTIVE, clusterStateChange.getNewState());
        assertEquals(ClusterState.ACTIVE, clusterStateChangeSameAttributes.getNewState());
        assertEquals(Version.UNKNOWN, clusterStateChangeOtherType.getNewState());
        assertEquals(ClusterState.FROZEN, clusterStateChangeOtherNewState.getNewState());
    }

    @Test
    public void testEquals() {
        assertEquals(clusterStateChange, clusterStateChange);
        assertEquals(clusterStateChange, clusterStateChangeSameAttributes);

        assertNotEquals(clusterStateChange, null);
        assertNotEquals(clusterStateChange, new Object());

        assertNotEquals(clusterStateChange, clusterStateChangeOtherType);
        assertNotEquals(clusterStateChange, clusterStateChangeOtherNewState);
    }

    @Test
    public void testHashCode() {
        assertEquals(clusterStateChange.hashCode(), clusterStateChange.hashCode());
        assertEquals(clusterStateChange.hashCode(), clusterStateChangeSameAttributes.hashCode());

        assertNotEquals(clusterStateChange.hashCode(), clusterStateChangeOtherType.hashCode());
        assertNotEquals(clusterStateChange.hashCode(), clusterStateChangeOtherNewState.hashCode());
    }
}
