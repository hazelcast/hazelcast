/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.client.impl.spi;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.ClientConnectionManagerFactory;
import com.hazelcast.client.impl.clientside.DefaultClientConnectionManagerFactory;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.connection.AddressProvider;
import com.hazelcast.client.impl.spi.impl.ClientClusterServiceImpl;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.InitialMembershipEvent;
import com.hazelcast.cluster.InitialMembershipListener;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.jet.pipeline.MockLoggingFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.hazelcast.internal.cluster.Versions.CURRENT_CLUSTER_VERSION;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public abstract class ClientClusterServiceBaseTest extends HazelcastTestSupport {

    protected static final MemberVersion VERSION
            = MemberVersion.of(BuildInfoProvider.getBuildInfo().getVersion());
    private static final String HAZELCAST_LOGGING_TYPE = "hazelcast.logging.type";
    private static final String HAZELCAST_LOGGING_CLASS = "hazelcast.logging.class";

    private String prevLoggingType;
    private String prevLoggingClass;
    protected HazelcastClientInstanceImpl client;
    private final ClientConnectionManagerFactory factory = new DefaultClientConnectionManagerFactory();
    private final AddressProvider addressProvider = mock(AddressProvider.class);

    protected abstract ClientClusterService createClientClusterService();


    @Before
    public void setUp() {
        prevLoggingType = System.getProperty(HAZELCAST_LOGGING_TYPE);
        prevLoggingClass = System.getProperty(HAZELCAST_LOGGING_CLASS);

        System.clearProperty(HAZELCAST_LOGGING_TYPE);
        System.setProperty(HAZELCAST_LOGGING_CLASS, MockLoggingFactory.class.getCanonicalName());
        client = new HazelcastClientInstanceImpl(UUID.randomUUID().toString(),
                new ClientConfig(), null, factory, addressProvider);
    }

    @After
    public void after() {
        if (prevLoggingType == null) {
            System.clearProperty(HAZELCAST_LOGGING_TYPE);
        } else {
            System.setProperty(HAZELCAST_LOGGING_TYPE, prevLoggingType);
        }
        if (prevLoggingClass == null) {
            System.clearProperty(HAZELCAST_LOGGING_CLASS);
        } else {
            System.setProperty(HAZELCAST_LOGGING_CLASS, prevLoggingClass);
        }
    }

    @Test
    public void testMemberAdded() {
        List<Member> members = new ArrayList<>();
        ClientClusterService clusterService = createClientClusterService();
        clusterService.addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                members.add(membershipEvent.getMember());
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) { }
        });
        MemberInfo member = member("127.0.0.1");
        UUID clusterUuid = UUID.randomUUID();
        // triggers initial event
        clusterService.handleMembersViewEvent(1, List.of(member), clusterUuid);
        // triggers member added
        MemberInfo memberInfo = member("127.0.0.2");
        clusterService.handleMembersViewEvent(2, List.of(member, memberInfo), clusterUuid);
        assertCollection(members, List.of(memberInfo.toMember()));
        assertEquals(2, clusterService.getMemberList().size());
    }

    @Test
    public void testMemberRemoved() {
        List<Member> members = new ArrayList<>();
        ClientClusterService clusterService = createClientClusterService();
        UUID clusterUuid = UUID.randomUUID();
        MemberInfo memberInfo = member("127.0.0.1");
        clusterService.handleMembersViewEvent(1, List.of(memberInfo), clusterUuid);
        clusterService.addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) { }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                members.add(membershipEvent.getMember());
            }
        });
        clusterService.handleMembersViewEvent(2, emptyList(), clusterUuid);
        assertCollection(members, List.of(memberInfo.toMember()));
        assertEquals(0, clusterService.getMemberList().size());
    }

    @Test
    public void testInitialMembershipListener_afterInitialListArrives() {
        AtomicInteger initialEventCount = new AtomicInteger();
        ClientClusterService clusterService = createClientClusterService();
        UUID clusterUuid = UUID.randomUUID();
        clusterService.handleMembersViewEvent(1, List.of(member("127.0.0.1")), clusterUuid);
        clusterService.addMembershipListener(new InitialMembershipListener() {
            @Override
            public void init(InitialMembershipEvent event) {
                initialEventCount.incrementAndGet();
            }

            @Override
            public void memberAdded(MembershipEvent membershipEvent) { }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) { }
        });
        assertEquals(1, initialEventCount.get());
    }

    @Test
    public void testInitialMembershipListener_beforeInitialListArrives() {
        AtomicInteger initialEventCount = new AtomicInteger();
        ClientClusterService clusterService = createClientClusterService();
        clusterService.addMembershipListener(new InitialMembershipListener() {
            @Override
            public void init(InitialMembershipEvent event) {
                initialEventCount.incrementAndGet();
            }

            @Override
            public void memberAdded(MembershipEvent membershipEvent) { }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) { }
        });
        clusterService.handleMembersViewEvent(1, List.of(member("127.0.0.1")), UUID.randomUUID());
        assertEquals(1, initialEventCount.get());
    }

    @Test
    public void testFireOnlyIncrementalEvents_afterClusterRestart() {
        AtomicInteger initialEventCount = new AtomicInteger();
        List<Member> addedMembers = new ArrayList<>();
        List<Member> removedMembers = new ArrayList<>();
        ClientClusterService clusterService = createClientClusterService();
        MemberInfo removedMemberInfo = member("127.0.0.1");
        clusterService.handleMembersViewEvent(1, List.of(removedMemberInfo), UUID.randomUUID());

        clusterService.addMembershipListener(new InitialMembershipListener() {
            @Override
            public void init(InitialMembershipEvent event) {
                initialEventCount.incrementAndGet();
            }

            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                addedMembers.add(membershipEvent.getMember());
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                removedMembers.add(membershipEvent.getMember());
            }
        });

        //called on cluster restart
        clusterService.onClusterConnect();

        MemberInfo addedMemberInfo = member("127.0.0.2");
        clusterService.handleMembersViewEvent(1, List.of(addedMemberInfo), UUID.randomUUID());
        assertEquals(1, clusterService.getMemberList().size());
        assertCollection(addedMembers, List.of(addedMemberInfo.toMember()));
        assertCollection(removedMembers, List.of(removedMemberInfo.toMember()));
        assertEquals(1, initialEventCount.get());
    }

    @Test
    public void testFireOnlyInitialEvent_afterClusterChange() {
        AtomicInteger initialEventCount = new AtomicInteger();
        AtomicInteger addedCount = new AtomicInteger();
        AtomicInteger removedCount = new AtomicInteger();
        ClientClusterService clusterService = createClientClusterService();
        clusterService.handleMembersViewEvent(1, List.of(member("127.0.0.1")), UUID.randomUUID());

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
        clusterService.onTryToConnectNextCluster();

        clusterService.handleMembersViewEvent(1, List.of(member("127.0.0.1")), UUID.randomUUID());
        assertEquals(1, clusterService.getMemberList().size());
        assertEquals(0, addedCount.get());
        assertEquals(0, removedCount.get());
        assertEquals(2, initialEventCount.get());
    }

    @Test
    public void testDontFire_whenReconnectToSameCluster() {
        AtomicInteger initialEventCount = new AtomicInteger();
        AtomicInteger addedCount = new AtomicInteger();
        AtomicInteger removedCount = new AtomicInteger();
        ClientClusterService clusterService = createClientClusterService();
        List<MemberInfo> memberList = List.of(member("127.0.0.1"));
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
        clusterService.onClusterConnect();

        clusterService.handleMembersViewEvent(1, memberList, clusterUuid);
        assertEquals(1, clusterService.getMemberList().size());
        assertEquals(0, addedCount.get());
        assertEquals(0, removedCount.get());
        assertEquals(1, initialEventCount.get());
    }


    /** Related to HotRestart where members keep their uuid's same but addresses changes. */
    @Test
    public void testFireEvents_whenAddressOfTheMembersChanges() {
        AtomicInteger initialEventCount = new AtomicInteger();
        List<Member> addedMembers = new ArrayList<>();
        List<Member> removedMembers = new ArrayList<>();
        ClientClusterService clusterService = createClientClusterService();
        UUID member1uuid = UUID.randomUUID();
        UUID member2uuid = UUID.randomUUID();
        UUID clusterUuid = UUID.randomUUID();
        MemberInfo removedMember1 = member("127.0.0.1", member1uuid);
        MemberInfo removedMember2 = member("127.0.0.2", member2uuid);
        clusterService.handleMembersViewEvent(1,
                List.of(removedMember1, removedMember2),
                clusterUuid);

        clusterService.addMembershipListener(new InitialMembershipListener() {
            @Override
            public void init(InitialMembershipEvent event) {
                initialEventCount.incrementAndGet();
            }

            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                addedMembers.add(membershipEvent.getMember());
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                removedMembers.add(membershipEvent.getMember());
            }
        });

        //called on reconnect to same cluster when registering the listener back
        clusterService.onClusterConnect();

        MemberInfo addedMember1 = member("127.0.0.1", member2uuid);
        MemberInfo addedMember2 = member("127.0.0.2", member1uuid);
        clusterService.handleMembersViewEvent(1,
                List.of(addedMember1, addedMember2),
                clusterUuid);
        assertEquals(2, clusterService.getMemberList().size());
        assertCollection(addedMembers, List.of(addedMember1.toMember(), addedMember2.toMember()));
        assertCollection(removedMembers, List.of(removedMember1.toMember(), removedMember2.toMember()));
        assertEquals(1, initialEventCount.get());
    }

    /** Related to HotRestart where members keep their uuid's and addresses same. */
    @Test
    public void testFireEvents_whenAddressAndUuidsDoesNotChange() {
        AtomicInteger initialEventCount = new AtomicInteger();
        List<Member> addedMembers = new ArrayList<>();
        List<Member> removedMembers = new ArrayList<>();
        ClientClusterService clusterService = createClientClusterService();
        UUID clusterUuid = UUID.randomUUID();
        MemberInfo member1 = member("127.0.0.1");
        MemberInfo member2 = member("127.0.0.2");
        List<MemberInfo> memberList = List.of(member1, member2);
        clusterService.handleMembersViewEvent(1, memberList, clusterUuid);

        clusterService.addMembershipListener(new InitialMembershipListener() {
            @Override
            public void init(InitialMembershipEvent event) {
                initialEventCount.incrementAndGet();
            }

            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                addedMembers.add(membershipEvent.getMember());
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                removedMembers.add(membershipEvent.getMember());
            }
        });

        //called on reconnect to same cluster when registering the listener back
        clusterService.onClusterConnect();

        clusterService.handleMembersViewEvent(1, memberList, UUID.randomUUID());
        assertEquals(2, clusterService.getMemberList().size());
        assertCollection(addedMembers, List.of(member1.toMember(), member2.toMember()));
        assertCollection(removedMembers, List.of(member1.toMember(), member2.toMember()));
        assertEquals(1, initialEventCount.get());

    }

    @Test
    public void testDontServeEmptyMemberList_duringClusterRestart() {
        ClientClusterService clusterService = createClientClusterService();
        clusterService.handleMembersViewEvent(1, List.of(member("127.0.0.1")), UUID.randomUUID());
        assertEquals(1, clusterService.getMemberList().size());
        //called on cluster restart
        clusterService.onClusterConnect();
        assertEquals(1, clusterService.getMemberList().size());
        clusterService.handleMembersViewEvent(1, List.of(member("127.0.0.2")), UUID.randomUUID());
        assertEquals(1, clusterService.getMemberList().size());
    }

    @Test
    public void testDontServeEmptyMemberList_duringClusterChange() {
        ClientClusterService clusterService = createClientClusterService();
        clusterService.handleMembersViewEvent(1, List.of(member("127.0.0.1")), UUID.randomUUID());
        assertEquals(1, clusterService.getMemberList().size());
        //called on cluster change
        clusterService.onTryToConnectNextCluster();
        assertEquals(1, clusterService.getMemberList().size());
        assertEquals(ClientClusterServiceImpl.INITIAL_MEMBER_LIST_VERSION, clusterService.getMemberListVersion());
        clusterService.handleMembersViewEvent(1, List.of(member("127.0.0.2")), UUID.randomUUID());
        assertEquals(1, clusterService.getMemberList().size());
    }

    @Nonnull
    static MemberInfo member(String host) {
        try {
            return new MemberInfo(new Address(host, 5701), UUID.randomUUID(), emptyMap(), false, VERSION);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Nonnull
    private static MemberInfo liteMember(String host) {
        try {
            return new MemberInfo(new Address(host, 5701), UUID.randomUUID(), emptyMap(), true, VERSION);
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

    @Test
    public void testListenersFromConfigWorking() {
        ClientClusterService clusterService = createClientClusterService();
        List<Member> addedMembers = new ArrayList<>();
        clusterService.start(List.of(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                addedMembers.add(membershipEvent.getMember());
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) { }
        }));
        MemberInfo member = member("127.0.0.1");
        UUID clusterUuid = UUID.randomUUID();
        // triggers initial event
        clusterService.handleMembersViewEvent(1, List.of(member), clusterUuid);
        // triggers member added
        MemberInfo addedMemberInfo = member("127.0.0.2");
        clusterService.handleMembersViewEvent(2, List.of(member, addedMemberInfo), clusterUuid);
        assertCollection(addedMembers, List.of(addedMemberInfo.toMember()));
    }

    @Test
    public void testRemoveListener() {
        AtomicInteger addedCount = new AtomicInteger();
        ClientClusterService clusterService = createClientClusterService();
        UUID listenerUuid = clusterService.addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                addedCount.incrementAndGet();
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) { }
        });
        assertTrue(clusterService.removeMembershipListener(listenerUuid));
        MemberInfo member = member("127.0.0.1");
        UUID clusterUuid = UUID.randomUUID();
        // triggers initial event
        clusterService.handleMembersViewEvent(1, List.of(member), clusterUuid);
        // triggers member added
        clusterService.handleMembersViewEvent(2, List.of(member, member("127.0.0.2")), clusterUuid);
        // we have removed the listener. No event should be fired to our listener
        assertEquals(0, addedCount.get());
    }

    @Test
    public void testRemoveNonExistingListener() {
        ClientClusterService clusterService = createClientClusterService();
        assertFalse(clusterService.removeMembershipListener(UUID.randomUUID()));
    }

    @Test
    public void testGetMasterMember() {
        ClientClusterService clusterService = createClientClusterService();
        MemberInfo masterMember = member("127.0.0.1");
        clusterService.handleMembersViewEvent(1, List.of(masterMember, member("127.0.0.2"),
                member("127.0.0.3")), UUID.randomUUID());
        assertEquals(masterMember.toMember(), clusterService.getMasterMember());
    }

    @Test
    public void testGetMember() {
        ClientClusterService clusterService = createClientClusterService();
        MemberInfo masterMember = member("127.0.0.1");
        UUID member2Uuid = UUID.randomUUID();
        MemberInfo member2 = member("127.0.0.2", member2Uuid);
        clusterService.handleMembersViewEvent(1, List.of(masterMember, member2), UUID.randomUUID());
        assertEquals(member2.toMember(), clusterService.getMember(member2Uuid));
    }

    @Test
    public void testGetMembers() {
        ClientClusterService clusterService = createClientClusterService();
        MemberInfo masterMember = member("127.0.0.1");
        MemberInfo liteMember = liteMember("127.0.0.2");
        MemberInfo dataMember = member("127.0.0.3");
        clusterService.handleMembersViewEvent(1, List.of(masterMember, liteMember, dataMember), UUID.randomUUID());
        assertCollection(List.of(masterMember.toMember(), liteMember.toMember(), dataMember.toMember()), clusterService.getMemberList());
        assertCollection(List.of(liteMember.toMember()), clusterService.getMembers(Member::isLiteMember));
        assertCollection(List.of(masterMember.toMember(), dataMember.toMember()), clusterService.getMembers(member -> !member.isLiteMember()));
    }

    @Test
    public void testGetEffectiveMemberList() {
        ClientClusterService clusterService = createClientClusterService();

        // Returns an empty list on startup, till the members view event
        assertCollection(emptyList(), clusterService.getEffectiveMemberList());

        List<MemberInfo> members = List.of(member("127.0.0.1"));

        for (int i = 0; i < 3; i++) {
            clusterService.handleMembersViewEvent(i, members, UUID.randomUUID());

            // Returns the member list after the members view event
            assertCollection(
                    members.stream()
                            .map(MemberInfo::toMember)
                            .collect(Collectors.toList()),
                    clusterService.getEffectiveMemberList());

            clusterService.onTryToConnectNextCluster();

            // Returns an empty list after reset
            assertCollection(emptyList(), clusterService.getEffectiveMemberList());
        }
    }

    @Test
    public void testGetClusterVersion_afterAuthentication() {
        ClientClusterService clusterService = createClientClusterService();
        assertEquals(Version.UNKNOWN, clusterService.getClusterVersion());

        clusterService.updateOnAuth(null, null, Map.of("clusterVersion", "5.5"));
        assertEquals(Version.of(5, 5), clusterService.getClusterVersion());
    }

    @Test
    public void testGetClusterVersion_afterEvent() {
        ClientClusterService clusterService = createClientClusterService();
        assertEquals(Version.UNKNOWN, clusterService.getClusterVersion());

        clusterService.handleClusterVersionEvent(CURRENT_CLUSTER_VERSION);
        assertEquals(CURRENT_CLUSTER_VERSION, clusterService.getClusterVersion());
    }

    @Test
    public void memberListLoggedOnHandleMembersViewEvent() {
        HazelcastClientInstanceImpl client = new HazelcastClientInstanceImpl(UUID.randomUUID().toString(),
                new ClientConfig(), null, factory, addressProvider);

        ClientClusterService clusterService = new ClientClusterServiceImpl(client);

        MemberInfo member = member("127.0.0.1");
        List<MemberInfo> members = List.of(member);

        String expectedLogMessage = """
                Members [1] {%n\
                \tMember [127.0.0.1]:5701 - %s%n\
                }%n\
                """.formatted(member.getUuid());

        for (int i = 0; i < 3; i++) {
            clusterService.handleMembersViewEvent(i, members, UUID.randomUUID());
            assertCollection(
                    members.stream().map(MemberInfo::toMember).collect(Collectors.toList()),
                    clusterService.getEffectiveMemberList()
            );
        }

        assertThat(MockLoggingFactory.capturedMessages)
                .filteredOn(msg -> msg.contains(expectedLogMessage))
                .hasSize(3);
    }
}
