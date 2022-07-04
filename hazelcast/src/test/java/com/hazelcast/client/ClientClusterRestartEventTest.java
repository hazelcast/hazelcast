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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.test.HazelcastTestSupport.assertContains;
import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.spawn;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientClusterRestartEventTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }

    protected Config newConfig() {
        return new Config();
    }

    private ClientConfig newClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        return clientConfig;
    }

    @Test
    public void testSingleMemberRestart() {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance(newConfig());
        Member oldMember = instance.getCluster().getLocalMember();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(newClientConfig());

        final CountDownLatch memberAdded = new CountDownLatch(1);
        final CountDownLatch memberRemoved = new CountDownLatch(1);
        final AtomicReference<Member> addedMemberReference = new AtomicReference<Member>();
        final AtomicReference<Member> removedMemberReference = new AtomicReference<Member>();
        client.getCluster().addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                addedMemberReference.set(membershipEvent.getMember());
                memberAdded.countDown();
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                removedMemberReference.set(membershipEvent.getMember());
                memberRemoved.countDown();
            }

        });

        instance.shutdown();
        //Allow same addresses to be used to test hot restart correctly
        hazelcastFactory.cleanup();
        instance = hazelcastFactory.newHazelcastInstance(newConfig());
        Member newMember = instance.getCluster().getLocalMember();

        assertOpenEventually(memberRemoved);
        assertEquals(oldMember, removedMemberReference.get());

        assertOpenEventually(memberAdded);
        assertEquals(newMember, addedMemberReference.get());

        Set<Member> members = client.getCluster().getMembers();
        assertContains(members, newMember);
        assertEquals(1, members.size());
    }

    @Test
    public void testMultiMemberRestart() throws ExecutionException, InterruptedException {
        HazelcastInstance instance1 = hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(newClientConfig());
        Member oldMember1 = instance1.getCluster().getLocalMember();
        Member oldMember2 = instance2.getCluster().getLocalMember();

        final CountDownLatch memberAdded = new CountDownLatch(2);
        final Set<Member> addedMembers = Collections.newSetFromMap(new ConcurrentHashMap<Member, Boolean>());
        final CountDownLatch memberRemoved = new CountDownLatch(2);
        final Set<Member> removedMembers = Collections.newSetFromMap(new ConcurrentHashMap<Member, Boolean>());
        client.getCluster().addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                addedMembers.add(membershipEvent.getMember());
                memberAdded.countDown();
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                removedMembers.add(membershipEvent.getMember());
                memberRemoved.countDown();

            }
        });

        Cluster cluster = instance1.getCluster();
        assertTrueEventually(() -> {
            try {
                cluster.shutdown();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
        //Allow same addresses to be used to test hot restart correctly
        hazelcastFactory.cleanup();
        Future<HazelcastInstance> f1 = spawn(() -> hazelcastFactory.newHazelcastInstance(newConfig()));
        Future<HazelcastInstance> f2 = spawn(() -> hazelcastFactory.newHazelcastInstance(newConfig()));

        instance1 = f1.get();
        instance2 = f2.get();

        Member newMember1 = instance1.getCluster().getLocalMember();
        Member newMember2 = instance2.getCluster().getLocalMember();

        assertOpenEventually(memberRemoved);
        assertEquals(2, removedMembers.size());
        assertContains(removedMembers, oldMember1);
        assertContains(removedMembers, oldMember2);

        assertOpenEventually(memberAdded);
        assertEquals(2, addedMembers.size());
        assertContains(addedMembers, newMember1);
        assertContains(addedMembers, newMember2);

        Set<Member> members = client.getCluster().getMembers();
        assertContains(members, newMember1);
        assertContains(members, newMember2);
        assertEquals(2, members.size());

    }
}
