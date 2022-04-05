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

package com.hazelcast.client.io;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.config.Config;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.map.IMap;
import com.hazelcast.partition.Partition;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.scheduledexecutor.IScheduledFuture;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.instance.EndpointQualifier.CLIENT;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertContains;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class AdvancedNetworkClientIntegrationTest {

    private static final int CLUSTER_SIZE = 3;
    private static final int BASE_CLIENT_PORT = 9090;

    private final HazelcastInstance[] instances = new HazelcastInstance[CLUSTER_SIZE];
    private HazelcastInstance client;

    @Before
    public void setup() {
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            instances[i] = Hazelcast.newHazelcastInstance(getConfig());
        }
        assertClusterSizeEventually(CLUSTER_SIZE, instances);
    }

    @After
    public void tearDown() {
        client.shutdown();
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            instances[i].getLifecycleService().terminate();
        }
    }

    @Test
    @Category(QuickTest.class)
    public void clientSmokeTest() {
        client = HazelcastClient.newHazelcastClient(getClientConfig());
        IMap<Integer, Integer> map = client.getMap("test");
        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }
        assertEquals(1000, map.size());
    }

    @Test
    public void testClientMembersList() {
        client = HazelcastClient.newHazelcastClient(getClientConfig());
        Set<Member> clientViewOfMembers = client.getCluster().getMembers();
        Set<Member> members = instances[1].getCluster().getMembers();

        Set<Address> clientViewOfAddresses = new HashSet<>();
        for (Member member : clientViewOfMembers) {
            clientViewOfAddresses.add(member.getAddress());
        }

        for (Member member : members) {
            assertContains(clientViewOfAddresses, member.getAddressMap().get(CLIENT));
        }
    }

    @Test
    public void testClientViewOfAddressMap() {
        client = HazelcastClient.newHazelcastClient(getClientConfig());
        for (Member member: client.getCluster().getMembers()) {
            assertEquals(member.getAddress(), member.getAddressMap().get(CLIENT));
            int memberPort = member.getAddressMap().get(EndpointQualifier.MEMBER).getPort();
            assertTrue("member address port is between 5700 and 6000", 5700  <= memberPort && memberPort <= 6000);
        }
    }

    @Test
    public void testClientMembershipEvent() {
        client = HazelcastClient.newHazelcastClient(getClientConfig());
        AtomicReference<Member> memberAdded = new AtomicReference<>();
        AtomicReference<Member> memberRemoved = new AtomicReference<>();
        Address removedMemberAddress = instances[2].getCluster().getLocalMember().getAddressMap().get(CLIENT);

        client.getCluster().addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                memberAdded.set(membershipEvent.getMember());
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                memberRemoved.set(membershipEvent.getMember());
            }

        });

        instances[2].shutdown();
        assertClusterSizeEventually(2, instances[0]);
        assertTrueEventually(() -> assertNotNull(memberRemoved.get()));

        assertEquals(memberRemoved.get().getAddress(), removedMemberAddress);

        instances[2] = Hazelcast.newHazelcastInstance(getConfig());
        assertClusterSizeEventually(3, instances);

        assertTrueEventually(() -> assertNotNull(memberAdded.get()));
        assertEquals(memberAdded.get().getAddress(), instances[2].getCluster().getLocalMember().getAddressMap().get(CLIENT));
    }

    @Test
    public void testPartitions() {
        client = HazelcastClient.newHazelcastClient(getClientConfig());
        Iterator<Partition> memberPartitions = instances[0].getPartitionService().getPartitions().iterator();
        Set<Partition> partitions = client.getPartitionService().getPartitions();

        for (Partition partition : partitions) {
            Partition memberPartition = memberPartitions.next();
            assertEquals(memberPartition.getPartitionId(), partition.getPartitionId());
            assertTrueEventually(() -> {
                Member owner = partition.getOwner();
                assertNotNull(owner);
                assertEquals(memberPartition.getOwner().getAddressMap().get(CLIENT), owner.getAddress());
            });
        }
    }

    @Test
    public void testGetScheduledFutures() throws Exception {
        client = HazelcastClient.newHazelcastClient(getClientConfig());
        IScheduledExecutorService executorService = client.getScheduledExecutorService("test");
        Member targetMember = client.getCluster().getMembers().iterator().next();

        IScheduledFuture<Address> future = executorService.scheduleOnMember(new ReportExecutionMember(),
                targetMember, 3, TimeUnit.SECONDS);
        Map<Member, List<IScheduledFuture<Address>>> futures = executorService.getAllScheduledFutures();

        assertContains(futures.keySet(), targetMember);

        IScheduledFuture<Address> futureOfMember = futures.get(targetMember).get(0);
        assertEquals(futureOfMember.get(), future.get());
    }

    @Test
    public void testScheduleOnMember() throws Exception {
        client = HazelcastClient.newHazelcastClient(getClientConfig());
        IScheduledExecutorService executorService = client.getScheduledExecutorService("test");
        Member targetMember = client.getCluster().getMembers().iterator().next();

        IScheduledFuture<Address> future = executorService.scheduleOnMember(new ReportExecutionMember(),
                targetMember, 3, TimeUnit.SECONDS);

        assertEquals(targetMember.getUuid(), future.getHandler().getUuid());

        Address clusterMemberAddress = null;
        for (HazelcastInstance instance : instances) {
            if (targetMember.getAddress().equals(instance.getCluster().getLocalMember().getAddressMap().get(CLIENT))) {
                clusterMemberAddress = instance.getCluster().getLocalMember().getAddress();
            }
        }

        assertEquals(future.get(), clusterMemberAddress);

    }

    public static class ReportExecutionMember implements Callable<Address>, Serializable, HazelcastInstanceAware {

        private volatile HazelcastInstance instance;

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.instance = hazelcastInstance;
        }

        @Override
        public Address call() {
            return instance.getCluster().getLocalMember().getAddress();
        }
    }

    private Config getConfig() {
        Config config = smallInstanceConfig();
        config.getAdvancedNetworkConfig().setEnabled(true)
              .setClientEndpointConfig(new ServerSocketEndpointConfig().setPort(BASE_CLIENT_PORT));
        config.getAdvancedNetworkConfig()
              .getJoin().getMulticastConfig().setEnabled(false);
        config.getAdvancedNetworkConfig()
              .getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        return config;
    }

    private ClientConfig getClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("127.0.0.1:9090");
        return clientConfig;
    }
}
