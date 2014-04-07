/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance;

import com.hazelcast.cluster.ClusterServiceImpl;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

final class NodeShutdownLatch {

    private final Map<String, HazelcastInstanceImpl> registrations;

    private final Semaphore latch;

    private final MemberImpl localMember;

    NodeShutdownLatch(final Node node) {
        localMember = node.localMember;
        Collection<MemberImpl> memberList = node.clusterService.getMemberList();
        registrations = new HashMap<String, HazelcastInstanceImpl>(3);
        Set<MemberImpl> members = new HashSet<MemberImpl>(memberList);
        members.remove(localMember);

        if (!members.isEmpty()) {
            final Map<MemberImpl, HazelcastInstanceImpl> map = HazelcastInstanceFactory.getInstanceImplMap();
            for (Map.Entry<MemberImpl, HazelcastInstanceImpl> entry : map.entrySet()) {
                final MemberImpl member = entry.getKey();
                if (members.contains(member)) {
                    HazelcastInstanceImpl instance = entry.getValue();
                    if (instance.node.isActive()) {
                        try {
                            ClusterServiceImpl clusterService = instance.node.clusterService;
                            final String id = clusterService.addMembershipListener(new ShutdownMembershipListener());
                            registrations.put(id, instance);
                        } catch (Throwable ignored) {
                        }
                    }
                }
            }
        }
        latch = new Semaphore(0);
    }

    void await(long time, TimeUnit unit) {
        if (registrations.isEmpty()) {
            return;
        }

        int permits = registrations.size();
        for (HazelcastInstanceImpl instance : registrations.values()) {
            if (!instance.node.isActive()) {
                permits--;
            }
        }
        try {
            latch.tryAcquire(permits, time, unit);
        } catch (InterruptedException ignored) {
        }

        for (Map.Entry<String, HazelcastInstanceImpl> entry : registrations.entrySet()) {
            final HazelcastInstanceImpl instance = entry.getValue();
            try {
                instance.node.clusterService.removeMembershipListener(entry.getKey());
            } catch (Throwable ignored) {
            }
        }
        registrations.clear();
    }

    private class ShutdownMembershipListener implements MembershipListener {
        @Override
        public void memberAdded(MembershipEvent membershipEvent) {
        }

        @Override
        public void memberRemoved(MembershipEvent event) {
            if (localMember.equals(event.getMember())) {
                latch.release();
            }
        }

        @Override
        public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
        }
    }
}
