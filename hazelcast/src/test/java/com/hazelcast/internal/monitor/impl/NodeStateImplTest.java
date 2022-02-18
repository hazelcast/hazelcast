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

package com.hazelcast.internal.monitor.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.internal.monitor.NodeState;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NodeStateImplTest {

    @Test
    public void toJson() throws Exception {
        ClusterState clusterState = ClusterState.ACTIVE;
        com.hazelcast.instance.impl.NodeState nodeState = com.hazelcast.instance.impl.NodeState.PASSIVE;
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
