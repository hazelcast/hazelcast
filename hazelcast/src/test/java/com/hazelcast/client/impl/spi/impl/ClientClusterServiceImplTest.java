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
package com.hazelcast.client.impl.spi.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.InitialMembershipEvent;
import com.hazelcast.cluster.InitialMembershipListener;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientClusterServiceImplTest {

    private static final MemberVersion VERSION = MemberVersion.of(BuildInfoProvider.getBuildInfo().getVersion());

    @Test
    public void testMemberAdded() {
        AtomicInteger addedCount = new AtomicInteger();
        ClientClusterServiceImpl clusterService = new ClientClusterServiceImpl(mock(ILogger.class));
        clusterService.addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                addedCount.incrementAndGet();
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {

            }
        });
        // triggers initial event
        MemberInfo member = member("127.0.0.1");
        UUID clusterUuid = UUID.randomUUID();
        clusterService.handleMembersViewEvent(1, asList(member), clusterUuid);
        // triggers member added
        clusterService.handleMembersViewEvent(2, asList(member, member("127.0.0.2")), clusterUuid);
        assertEquals(1, addedCount.get());
        assertEquals(2, clusterService.getMemberList().size());
    }

    @Test
    public void testMemberRemoved() {
        AtomicInteger removedCount = new AtomicInteger();
        ClientClusterServiceImpl clusterService = new ClientClusterServiceImpl(mock(ILogger.class));
        UUID clusterUuid = UUID.randomUUID();
        clusterService.handleMembersViewEvent(1, asList(member("127.0.0.1")), clusterUuid);
        clusterService.addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                removedCount.incrementAndGet();
            }
        });
        clusterService.handleMembersViewEvent(2, Collections.emptyList(), clusterUuid);
        assertEquals(1, removedCount.get());
        assertEquals(0, clusterService.getMemberList().size());
    }

    @Test
    public void testInitialMembershipListener_AfterInitialListArrives() {
        AtomicInteger initialEventCount = new AtomicInteger();
        ClientClusterServiceImpl clusterService = new ClientClusterServiceImpl(mock(ILogger.class));
        UUID clusterUuid = UUID.randomUUID();
        clusterService.handleMembersViewEvent(1, asList(member("127.0.0.1")), clusterUuid);
        clusterService.addMembershipListener(new InitialMembershipListener() {
            @Override
            public void init(InitialMembershipEvent event) {
                initialEventCount.incrementAndGet();
            }

            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
            }
        });
        assertEquals(1, initialEventCount.get());
    }

    @Test
    public void testInitialMembershipListener_BeforeInitialListArrives() {
        AtomicInteger initialEventCount = new AtomicInteger();
        ClientClusterServiceImpl clusterService = new ClientClusterServiceImpl(mock(ILogger.class));
        clusterService.addMembershipListener(new InitialMembershipListener() {
            @Override
            public void init(InitialMembershipEvent event) {
                initialEventCount.incrementAndGet();
            }

            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
            }
        });
        clusterService.handleMembersViewEvent(1, asList(member("127.0.0.1")), UUID.randomUUID());
        assertEquals(1, initialEventCount.get());
    }

    @Test
    public void testFireOnlyIncrementalEvents_AfterClusterRestart() {
        AtomicInteger initialEventCount = new AtomicInteger();
        AtomicInteger addedCount = new AtomicInteger();
        AtomicInteger removedCount = new AtomicInteger();
        ClientClusterServiceImpl clusterService = new ClientClusterServiceImpl(mock(ILogger.class));
        clusterService.handleMembersViewEvent(1, asList(member("127.0.0.1")), UUID.randomUUID());

        clusterService.addMembershipListener(new InitialMembershipListener() {
            @Override
            public void init(InitialMembershipEvent event) {
                initialEventCount.incrementAndGet();
            }

            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                addedCount.incrementAndGet();
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                removedCount.incrementAndGet();
            }
        });

        //called on cluster restart
        clusterService.onClusterRestart();

        clusterService.handleMembersViewEvent(1, asList(member("127.0.0.2")), UUID.randomUUID());
        assertEquals(1, clusterService.getMemberList().size());
        assertEquals(1, addedCount.get());
        assertEquals(1, removedCount.get());
        assertEquals(1, initialEventCount.get());
    }

    @Test
    public void testFireOnlyInitialEvent_AfterClusterChange() {
        AtomicInteger initialEventCount = new AtomicInteger();
        AtomicInteger addedCount = new AtomicInteger();
        AtomicInteger removedCount = new AtomicInteger();
        ClientClusterServiceImpl clusterService = new ClientClusterServiceImpl(mock(ILogger.class));
        clusterService.handleMembersViewEvent(1, asList(member("127.0.0.1")), UUID.randomUUID());

        clusterService.addMembershipListener(new InitialMembershipListener() {
            @Override
            public void init(InitialMembershipEvent event) {
                initialEventCount.incrementAndGet();
            }

            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                addedCount.incrementAndGet();
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                removedCount.incrementAndGet();
            }
        });

        //called on cluster change
        clusterService.onClusterChange();

        clusterService.handleMembersViewEvent(1, asList(member("127.0.0.1")), UUID.randomUUID());
        assertEquals(1, clusterService.getMemberList().size());
        assertEquals(0, addedCount.get());
        assertEquals(0, removedCount.get());
        assertEquals(2, initialEventCount.get());
    }

    @Test
    public void testDontFire_WhenReconnectToSameCluster() {
        AtomicInteger initialEventCount = new AtomicInteger();
        AtomicInteger addedCount = new AtomicInteger();
        AtomicInteger removedCount = new AtomicInteger();
        ClientClusterServiceImpl clusterService = new ClientClusterServiceImpl(mock(ILogger.class));
        List<MemberInfo> memberList = asList(member("127.0.0.1"));
        UUID clusterUuid = UUID.randomUUID();
        clusterService.handleMembersViewEvent(1, memberList, clusterUuid);

        clusterService.addMembershipListener(new InitialMembershipListener() {
            @Override
            public void init(InitialMembershipEvent event) {
                initialEventCount.incrementAndGet();
            }

            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                addedCount.incrementAndGet();
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                removedCount.incrementAndGet();
            }
        });

        //called on reconnect to same cluster when registering the listener back
        clusterService.onClusterRestart();

        clusterService.handleMembersViewEvent(1, memberList, clusterUuid);
        assertEquals(1, clusterService.getMemberList().size());
        assertEquals(0, addedCount.get());
        assertEquals(0, removedCount.get());
        assertEquals(1, initialEventCount.get());
    }


    @Test
    /*
      Related to HotRestart where members keep their uuid's same but addresses changes.
     */
    public void testFireEvents_WhenAddressOfTheMembersChanges() {
        AtomicInteger initialEventCount = new AtomicInteger();
        AtomicInteger addedCount = new AtomicInteger();
        AtomicInteger removedCount = new AtomicInteger();
        ClientClusterServiceImpl clusterService = new ClientClusterServiceImpl(mock(ILogger.class));
        UUID member1uuid = UUID.randomUUID();
        UUID member2uuid = UUID.randomUUID();
        UUID clusterUuid = UUID.randomUUID();
        clusterService.handleMembersViewEvent(1,
                asList(member("127.0.0.1", member1uuid), member("127.0.0.2", member2uuid)),
                clusterUuid);

        clusterService.addMembershipListener(new InitialMembershipListener() {
            @Override
            public void init(InitialMembershipEvent event) {
                initialEventCount.incrementAndGet();
            }

            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                addedCount.incrementAndGet();
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                removedCount.incrementAndGet();
            }
        });

        //called on reconnect to same cluster when registering the listener back
        clusterService.onClusterRestart();

        clusterService.handleMembersViewEvent(1,
                asList(member("127.0.0.1", member2uuid), member("127.0.0.2", member1uuid)),
                clusterUuid);
        assertEquals(2, clusterService.getMemberList().size());
        assertEquals(2, addedCount.get());
        assertEquals(2, removedCount.get());
        assertEquals(1, initialEventCount.get());

    }

    @Test
    /*
      Related to HotRestart where members keep their uuid's and addresses same.
     */
    public void testFireEvents_WhenAddressAndUuidsDoesNotChange() {
        AtomicInteger initialEventCount = new AtomicInteger();
        AtomicInteger addedCount = new AtomicInteger();
        AtomicInteger removedCount = new AtomicInteger();
        ClientClusterServiceImpl clusterService = new ClientClusterServiceImpl(mock(ILogger.class));
        UUID clusterUuid = UUID.randomUUID();
        List<MemberInfo> memberList = asList(member("127.0.0.1"), member("127.0.0.2"));
        clusterService.handleMembersViewEvent(1, memberList, clusterUuid);

        clusterService.addMembershipListener(new InitialMembershipListener() {
            @Override
            public void init(InitialMembershipEvent event) {
                initialEventCount.incrementAndGet();
            }

            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                addedCount.incrementAndGet();
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                removedCount.incrementAndGet();
            }
        });

        //called on reconnect to same cluster when registering the listener back
        clusterService.onClusterRestart();

        clusterService.handleMembersViewEvent(1, memberList, UUID.randomUUID());
        assertEquals(2, clusterService.getMemberList().size());
        assertEquals(2, addedCount.get());
        assertEquals(2, removedCount.get());
        assertEquals(1, initialEventCount.get());

    }

    @Test
    public void testDontServeEmptyMemberList_DuringClusterRestart() {
        ClientClusterServiceImpl clusterService = new ClientClusterServiceImpl(mock(ILogger.class));
        clusterService.handleMembersViewEvent(1, asList(member("127.0.0.1")), UUID.randomUUID());
        assertEquals(1, clusterService.getMemberList().size());
        //called on cluster restart
        clusterService.onClusterRestart();
        assertEquals(1, clusterService.getMemberList().size());
        clusterService.handleMembersViewEvent(1, asList(member("127.0.0.2")), UUID.randomUUID());
        assertEquals(1, clusterService.getMemberList().size());
    }

    @Test
    public void testDontServeEmptyMemberList_DuringClusterChange() {
        ClientClusterServiceImpl clusterService = new ClientClusterServiceImpl(mock(ILogger.class));
        clusterService.handleMembersViewEvent(1, asList(member("127.0.0.1")), UUID.randomUUID());
        assertEquals(1, clusterService.getMemberList().size());
        //called on cluster change
        clusterService.onClusterChange();
        assertEquals(1, clusterService.getMemberList().size());
        clusterService.handleMembersViewEvent(1, asList(member("127.0.0.2")), UUID.randomUUID());
        assertEquals(1, clusterService.getMemberList().size());
    }

    @Nonnull
    private static MemberInfo member(String host) {
        try {
            return new MemberInfo(new Address(host, 5701), UUID.randomUUID(), emptyMap(), false, VERSION);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Nonnull
    private static MemberInfo member(String host, UUID uuid) {
        try {
            return new MemberInfo(new Address(host, 5701), uuid, emptyMap(), false, VERSION);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

}
