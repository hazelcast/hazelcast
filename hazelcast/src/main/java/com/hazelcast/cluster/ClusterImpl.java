/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cluster;

import com.hazelcast.core.Cluster;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.util.Clock;
import com.hazelcast.impl.MemberImpl;
import com.hazelcast.impl.Node;
import com.hazelcast.nio.Address;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReference;

public class ClusterImpl implements Cluster {

    final CopyOnWriteArraySet<MembershipListener> listeners = new CopyOnWriteArraySet<MembershipListener>();
    final AtomicReference<Set<Member>> members = new AtomicReference<Set<Member>>();
    final AtomicReference<Member> localMember = new AtomicReference<Member>();
    final Map<Member, Member> clusterMembers = new ConcurrentHashMap<Member, Member>();
    final Map<Address, Member> mapMembers = new ConcurrentHashMap<Address, Member>();
    @SuppressWarnings("VolatileLongOrDoubleField")
    volatile long clusterTimeDiff = Long.MAX_VALUE;
    final Node node;

    public ClusterImpl(Node node) {
        this.node = node;
        this.setMembers(Arrays.asList(node.getLocalMember()));
    }

    public void reset() {
        mapMembers.clear();
        clusterMembers.clear();
        members.set(null);
        this.setMembers(Arrays.asList(node.getLocalMember()));
    }

    public void setMembers(List<MemberImpl> lsMembers) {
        Set<Member> setNew = new LinkedHashSet<Member>(lsMembers.size());
        ArrayList<Runnable> notifications = new ArrayList<Runnable>();
        for (MemberImpl member : lsMembers) {
            if (member != null) {
                final MemberImpl dummy = new MemberImpl(member.getAddress(), member.localMember(),
                        member.getNodeType(), member.getUuid());
                Member clusterMember = clusterMembers.get(dummy);
                if (clusterMember == null) {
                    clusterMember = dummy;
                    if (listeners.size() > 0) {
                        notifications.add(new Runnable() {
                            public void run() {
                                MembershipEvent membershipEvent = new MembershipEvent(ClusterImpl.this,
                                        dummy, MembershipEvent.MEMBER_ADDED);
                                for (MembershipListener listener : listeners) {
                                    listener.memberAdded(membershipEvent);
                                }
                            }
                        });
                    }
                }
                if (clusterMember.localMember()) {
                    localMember.set(clusterMember);
                }
                setNew.add(clusterMember);
            }
        }
        if (listeners.size() > 0) {
            Set<Member> it = clusterMembers.keySet();
            // build a list of notifications but send them AFTER removal
            for (final Member member : it) {
                if (!setNew.contains(member)) {
                    notifications.add(new Runnable() {
                        public void run() {
                            MembershipEvent membershipEvent = new MembershipEvent(ClusterImpl.this,
                                    member, MembershipEvent.MEMBER_REMOVED);
                            for (MembershipListener listener : listeners) {
                                listener.memberRemoved(membershipEvent);
                            }
                        }
                    });
                }
            }
        }
        clusterMembers.clear();
        mapMembers.clear();
        for (Member member : setNew) {
            mapMembers.put(((MemberImpl) member).getAddress(), member);
            clusterMembers.put(member, member);
        }
        members.set(Collections.unmodifiableSet(setNew));
        // send notifications now
        for (Runnable notification : notifications) {
            node.executorManager.getEventExecutorService().execute(notification);
        }
    }

    public void addMembershipListener(MembershipListener listener) {
        listeners.add(listener);
    }

    public void removeMembershipListener(MembershipListener listener) {
        listeners.remove(listener);
    }

    public Member getLocalMember() {
        return localMember.get();
    }

    public Set<Member> getMembers() {
        return members.get();
    }

    public long getClusterTime() {
        return Clock.currentTimeMillis() + ((clusterTimeDiff == Long.MAX_VALUE) ? 0 : clusterTimeDiff);
    }

    public void setMasterTime(long masterTime) {
        long diff = masterTime - Clock.currentTimeMillis();
        if (Math.abs(diff) < Math.abs(clusterTimeDiff)) {
            this.clusterTimeDiff = diff;
        }
    }

    public long getClusterTimeFor(long localTime) {
        return localTime + ((clusterTimeDiff == Long.MAX_VALUE) ? 0 : clusterTimeDiff);
    }

    public Member getMember(Address address) {
        return mapMembers.get(address);
    }

    @Override
    public String toString() {
        Set<Member> members = getMembers();
        StringBuffer sb = new StringBuffer("Cluster [");
        if (members != null) {
            sb.append(members.size());
            sb.append("] {");
            for (Member member : members) {
                sb.append("\n\t").append(member);
            }
        }
        sb.append("\n}\n");
        return sb.toString();
    }
}
