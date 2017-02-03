package com.hazelcast.monitor.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.monitor.NodeState;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import com.hazelcast.version.MemberVersion;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NodeStateImplTest {

    @Test
    public void toJson() throws Exception {
        ClusterState clusterState = ClusterState.ACTIVE;
        com.hazelcast.instance.NodeState nodeState = com.hazelcast.instance.NodeState.PASSIVE;
        Version clusterVersion = Version.of("3.8");
        MemberVersion memberVersion = MemberVersion.of("3.9.0");

        NodeState state = new NodeStateImpl(clusterState, nodeState, clusterVersion, memberVersion);
        NodeState deserialized = new NodeStateImpl();
        deserialized.fromJson(state.toJson());

        assertEquals(clusterState, deserialized.getClusterState());
        assertEquals(nodeState, deserialized.getNodeState());
        assertEquals(clusterVersion, deserialized.getClusterVersion());
        assertEquals(memberVersion, deserialized.getMemberVersion());
    }

}
