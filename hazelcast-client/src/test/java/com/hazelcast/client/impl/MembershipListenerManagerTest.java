/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client.impl;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.Packet;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.impl.MemberImpl;
import com.hazelcast.nio.Address;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.Serializer.toByte;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MembershipListenerManagerTest {
    @Test
    public void testRegisterMembershipListener() throws Exception {
        HazelcastClient client = mock(HazelcastClient.class);
        MembershipListenerManager listenerManager = new MembershipListenerManager(client);
        MembershipListener listener = new MembershipListener() {

            public void memberAdded(MembershipEvent membershipEvent) {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            public void memberRemoved(MembershipEvent membershipEvent) {
                //To change body of implemented methods use File | Settings | File Templates.
            }
        };
        listenerManager.registerMembershipListener(listener);
        assertFalse(listenerManager.noMembershipListenerRegistered());
    }

    @Test
    public void testRemoveMembershipListener() throws Exception {
        HazelcastClient client = mock(HazelcastClient.class);
        MembershipListenerManager listenerManager = new MembershipListenerManager(client);
        MembershipListener listener = new MembershipListener() {

            public void memberAdded(MembershipEvent membershipEvent) {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            public void memberRemoved(MembershipEvent membershipEvent) {
                //To change body of implemented methods use File | Settings | File Templates.
            }
        };
        listenerManager.registerMembershipListener(listener);
        listenerManager.removeMembershipListener(listener);
        assertTrue(listenerManager.noMembershipListenerRegistered());
    }

    @Test
    public void testNotifyMembershipListenerMemberAdded() throws Exception {
        notifyMembershipListener(MembershipEvent.MEMBER_ADDED);
    }

    @Test
    public void testNotifyMembershipListenerMemberRemoved() throws Exception {
        notifyMembershipListener(MembershipEvent.MEMBER_REMOVED);
    }

    private void notifyMembershipListener(final int type) throws InterruptedException {
        HazelcastClient client = mock(HazelcastClient.class);
        Cluster cluster = mock(Cluster.class);
        when(client.getCluster()).thenReturn(cluster);
        final MembershipListenerManager membershipListenerManager = new MembershipListenerManager(client);
        final CountDownLatch memberAdded = new CountDownLatch(1);
        final CountDownLatch memberRemoved = new CountDownLatch(1);
        MembershipListener listener = new MembershipListener() {

            public void memberAdded(MembershipEvent membershipEvent) {
                memberAdded.countDown();
            }

            public void memberRemoved(MembershipEvent membershipEvent) {
                memberRemoved.countDown();
            }
        };
        membershipListenerManager.registerMembershipListener(listener);
        new Thread(new Runnable() {

            public void run() {
                Packet packet = new Packet();
                Address address = new Address();
                Member member = new MemberImpl(address, false);
                packet.setKey(toByte(member));
                packet.setValue(toByte(type));
                membershipListenerManager.notifyMembershipListeners(packet);
            }
        }).start();
        if (type == MembershipEvent.MEMBER_ADDED) {
            assertTrue(memberAdded.await(5, TimeUnit.SECONDS));
        } else {
            assertTrue(memberRemoved.await(5, TimeUnit.SECONDS));
        }
    }
}
