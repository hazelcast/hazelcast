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

package com.hazelcast.internal.serialization.impl.compact.integration;

import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.impl.compact.CompactTestUtil;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import example.serialization.EmployeeDTO;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.Set;

import static com.hazelcast.test.SplitBrainTestSupport.blockCommunicationBetween;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCompactSplitBrainTest extends ClientTestSupport {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void tearDown() throws Exception {
        factory.terminateAll();
    }

    @Test
    public void testWithSplitBrain_clientReconnectsToOtherHalf() {
        Config config = smallInstanceConfig();

        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(2, instance1, instance2);

        // split the cluster
        blockCommunicationBetween(instance1, instance2);

        // make sure that each member quickly drops the other from their member list
        suspectMember(instance1, instance2);
        suspectMember(instance2, instance1);

        assertClusterSizeEventually(1, instance1);
        assertClusterSizeEventually(1, instance2);

        HazelcastInstance client = factory.newHazelcastClient();
        Set<Member> members = client.getCluster().getMembers();
        assertEquals(1, members.size());

        HazelcastInstance connectedMember;
        if (members.iterator().next().getUuid().equals(instance1.getLocalEndpoint().getUuid())) {
            connectedMember = instance1;
        } else {
            connectedMember = instance2;
        }

        // Put a compact serializable object
        IMap<Integer, EmployeeDTO> map = client.getMap("test");
        map.put(1, new EmployeeDTO(1, 1));

        CompactTestUtil.assertSchemasAvailable(Collections.singletonList(connectedMember), EmployeeDTO.class);

        ReconnectListener reconnectListener = new ReconnectListener();
        client.getLifecycleService().addLifecycleListener(reconnectListener);

        connectedMember.shutdown();

        assertOpenEventually(reconnectListener.reconnectedLatch);

        HazelcastInstance reconnectedMember = connectedMember == instance1
                ? instance2
                : instance1;

        // It might take a while until the state is sent from the client to the
        // reconnected member
        assertTrueEventually(() -> {
            CompactTestUtil.assertSchemasAvailable(Collections.singletonList(reconnectedMember), EmployeeDTO.class);
        });
    }
}
