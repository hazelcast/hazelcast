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
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientClusterServiceImplTest extends HazelcastTestSupport {

    private static final MemberVersion VERSION = MemberVersion.of(BuildInfoProvider.getBuildInfo().getVersion());

    @Test
    public void testMemberAdded() {
        LinkedList<Member> members = new LinkedList<>();
        ClientClusterServiceImpl clusterService = new ClientClusterServiceImpl(mock(ILogger.class));
        clusterService.addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                members.add(membershipEvent.getMember());
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {

            }
        });
        MemberInfo member = member("127.0.0.1");
        UUID clusterUuid = UUID.randomUUID();
        // triggers initial event
        clusterService.handleMembersViewEvent(1, asList(member), clusterUuid);
        // triggers member added
        MemberInfo memberInfo = member("127.0.0.2");
        clusterService.handleMembersViewEvent(2, asList(member, memberInfo), clusterUuid);
        assertCollection(members, Collections.singleton(memberInfo.toMember()));
        assertEquals(2, clusterService.getMemberList().size());
    }

    @Test
    public void testMemberRemoved() {
        LinkedList<Member> members = new LinkedList<>();
        ClientClusterServiceImpl clusterService = new ClientClusterServiceImpl(mock(ILogger.class));
        UUID clusterUuid = UUID.randomUUID();
        MemberInfo memberInfo = member("127.0.0.1");
        clusterService.handleMembersViewEvent(1, asList(memberInfo), clusterUuid);
        clusterService.addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                members.add(membershipEvent.getMember());
            }
        });
        clusterService.handleMembersViewEvent(2, Collections.emptyList(), clusterUuid);
        assertCollection(members, Collections.singleton(memberInfo.toMember()));
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
        LinkedList<Member> addedMembers = new LinkedList<>();
        LinkedList<Member> removedMembers = new LinkedList<>();
        ClientClusterServiceImpl clusterService = new ClientClusterServiceImpl(mock(ILogger.class));
        MemberInfo removedMemberInfo = member("127.0.0.1");
        clusterService.handleMembersViewEvent(1, asList(removedMemberInfo), UUID.randomUUID());

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
        clusterService.handleMembersViewEvent(1, asList(addedMemberInfo), UUID.randomUUID());
        assertEquals(1, clusterService.getMemberList().size());
        assertCollection(addedMembers, Collections.singleton(addedMemberInfo.toMember()));
        assertCollection(removedMembers, Collections.singleton(removedMemberInfo.toMember()));
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
        clusterService.onClusterConnect();

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
        LinkedList<Member> addedMembers = new LinkedList<>();
        LinkedList<Member> removedMembers = new LinkedList<>();
        ClientClusterServiceImpl clusterService = new ClientClusterServiceImpl(mock(ILogger.class));
        UUID member1uuid = UUID.randomUUID();
        UUID member2uuid = UUID.randomUUID();
        UUID clusterUuid = UUID.randomUUID();
        MemberInfo removedMember1 = member("127.0.0.1", member1uuid);
        MemberInfo removedMember2 = member("127.0.0.2", member2uuid);
        clusterService.handleMembersViewEvent(1,
                asList(removedMember1, removedMember2),
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
                asList(addedMember1, addedMember2),
                clusterUuid);
        assertEquals(2, clusterService.getMemberList().size());
        assertCollection(addedMembers, Arrays.asList(addedMember1.toMember(), addedMember2.toMember()));
        assertCollection(removedMembers, Arrays.asList(removedMember1.toMember(), removedMember2.toMember()));
        assertEquals(1, initialEventCount.get());
    }

    @Test
    /*
      Related to HotRestart where members keep their uuid's and addresses same.
     */
    public void testFireEvents_WhenAddressAndUuidsDoesNotChange() {
        AtomicInteger initialEventCount = new AtomicInteger();
        LinkedList<Member> addedMembers = new LinkedList<>();
        LinkedList<Member> removedMembers = new LinkedList<>();
        ClientClusterServiceImpl clusterService = new ClientClusterServiceImpl(mock(ILogger.class));
        UUID clusterUuid = UUID.randomUUID();
        MemberInfo member1 = member("127.0.0.1");
        MemberInfo member2 = member("127.0.0.2");
        List<MemberInfo> memberList = asList(member1, member2);
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
        assertCollection(addedMembers, Arrays.asList(member1.toMember(), member2.toMember()));
        assertCollection(removedMembers, Arrays.asList(member1.toMember(), member2.toMember()));
        assertEquals(1, initialEventCount.get());

    }

    @Test
    public void testDontServeEmptyMemberList_DuringClusterRestart() {
        ClientClusterServiceImpl clusterService = new ClientClusterServiceImpl(mock(ILogger.class));
        clusterService.handleMembersViewEvent(1, asList(member("127.0.0.1")), UUID.randomUUID());
        assertEquals(1, clusterService.getMemberList().size());
        //called on cluster restart
        clusterService.onClusterConnect();
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
        assertEquals(ClientClusterServiceImpl.INITIAL_MEMBER_LIST_VERSION, clusterService.getMemberListVersion());
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
        ClientClusterServiceImpl clusterService = new ClientClusterServiceImpl(mock(ILogger.class));
        LinkedList<Member> addedMembers = new LinkedList<>();
        clusterService.start(singleton(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                addedMembers.add(membershipEvent.getMember());
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {

            }
        }));
        MemberInfo member = member("127.0.0.1");
        UUID clusterUuid = UUID.randomUUID();
        // triggers initial event
        clusterService.handleMembersViewEvent(1, asList(member), clusterUuid);
        // triggers member added
        MemberInfo addedMemberInfo = member("127.0.0.2");
        clusterService.handleMembersViewEvent(2, asList(member, addedMemberInfo), clusterUuid);
        assertCollection(addedMembers, Collections.singleton(addedMemberInfo.toMember()));
    }

    @Test
    public void testRemoveListener() {
        AtomicInteger addedCount = new AtomicInteger();
        ClientClusterServiceImpl clusterService = new ClientClusterServiceImpl(mock(ILogger.class));
        UUID listenerUuid = clusterService.addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                addedCount.incrementAndGet();
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {

            }
        });
        assertTrue(clusterService.removeMembershipListener(listenerUuid));
        MemberInfo member = member("127.0.0.1");
        UUID clusterUuid = UUID.randomUUID();
        // triggers initial event
        clusterService.handleMembersViewEvent(1, asList(member), clusterUuid);
        // triggers member added
        clusterService.handleMembersViewEvent(2, asList(member, member("127.0.0.2")), clusterUuid);
        // we have removed the listener. No event should be fired to our listener
        assertEquals(0, addedCount.get());
    }

    @Test
    public void testRemoveNonExistingListener() {
        ClientClusterServiceImpl clusterService = new ClientClusterServiceImpl(mock(ILogger.class));
        assertFalse(clusterService.removeMembershipListener(UUID.randomUUID()));
    }

    @Test
    public void testGetMasterMember() {
        ClientClusterServiceImpl clusterService = new ClientClusterServiceImpl(mock(ILogger.class));
        MemberInfo masterMember = member("127.0.0.1");
        clusterService.handleMembersViewEvent(1, asList(masterMember, member("127.0.0.2"),
                member("127.0.0.3")), UUID.randomUUID());
        assertEquals(masterMember.toMember(), clusterService.getMasterMember());
    }

    @Test
    public void testGetMember() {
        ClientClusterServiceImpl clusterService = new ClientClusterServiceImpl(mock(ILogger.class));
        MemberInfo masterMember = member("127.0.0.1");
        UUID member2Uuid = UUID.randomUUID();
        MemberInfo member2 = member("127.0.0.2", member2Uuid);
        clusterService.handleMembersViewEvent(1, asList(masterMember, member2), UUID.randomUUID());
        assertEquals(member2.toMember(), clusterService.getMember(member2Uuid));
    }

    @Test
    public void testGetMembers() {
        ClientClusterServiceImpl clusterService = new ClientClusterServiceImpl(mock(ILogger.class));
        MemberInfo masterMember = member("127.0.0.1");
        MemberInfo liteMember = liteMember("127.0.0.2");
        MemberInfo dataMember = member("127.0.0.3");
        clusterService.handleMembersViewEvent(1, asList(masterMember, liteMember,
                dataMember), UUID.randomUUID());
        assertCollection(Arrays.asList(masterMember.toMember(), liteMember.toMember(), dataMember.toMember()), clusterService.getMemberList());
        assertCollection(Arrays.asList(liteMember.toMember()), clusterService.getMembers(Member::isLiteMember));
        assertCollection(Arrays.asList(masterMember.toMember(), dataMember.toMember()), clusterService.getMembers(member -> !member.isLiteMember()));
    }

    @Test
    public void testWaitInitialMembership() {
        ClientClusterServiceImpl clusterService = new ClientClusterServiceImpl(mock(ILogger.class));
        MemberInfo masterMember = member("127.0.0.1");
        clusterService.handleMembersViewEvent(1, asList(masterMember, liteMember("127.0.0.2"),
                member("127.0.0.3")), UUID.randomUUID());
        clusterService.waitInitialMemberListFetched();
    }
}
