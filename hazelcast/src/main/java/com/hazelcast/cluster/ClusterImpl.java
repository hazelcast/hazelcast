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

package com.hazelcast.cluster;

import com.hazelcast.core.Cluster;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.impl.MemberImpl;
import com.hazelcast.impl.Node;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReference;

public class ClusterImpl implements Cluster {

    final CopyOnWriteArraySet<MembershipListener> listeners = new CopyOnWriteArraySet<MembershipListener>();
    final AtomicReference<Set<Member>> members = new AtomicReference<Set<Member>>();
    final AtomicReference<Member> localMember = new AtomicReference<Member>();
    final Map<Member, Member> clusterMembers = new ConcurrentHashMap<Member, Member>();
    final Map<Member, Integer> distances = new ConcurrentHashMap<Member, Integer>();
    volatile long clusterTimeDiff = Long.MAX_VALUE;
    final Node node;

    public ClusterImpl(Node node, MemberImpl thisMember) {
        this.node = node;
        List ls = new ArrayList();
        ls.add(thisMember);
        this.setMembers(ls);
    }

    public int getDistanceFrom(Member member) {
        if (member.localMember()) {
            return 0;
        }
        Integer distance = distances.get(member);
        if (distance != null) {
            return distance;
        }
        Set<Member> currentMembers = members.get();
        int size = currentMembers.size();
        Member localMember = getLocalMember();
        int index = 0;
        int toIndex = getIndexOf(localMember, currentMembers);
        for (Member m : currentMembers) {
            if (!m.equals(localMember)) {
                distance = ((toIndex - index) + size) % size;
                distances.put(m, distance);
            }
            index++;
        }
        Integer d = distances.get(member);
        return (d == null) ? -1 : d;
    }

    private int getIndexOf(Member member, Set<Member> memberSet) {
        int count = 0;
        for (Member m : memberSet) {
            if (m.equals(member)) {
                return count;
            }
            count++;
        }
        return count;
    }

    public void setMembers(List<MemberImpl> lsMembers) {
        Set<Member> setNew = new LinkedHashSet<Member>(lsMembers.size());
        ArrayList<Runnable> notifications = new ArrayList<Runnable>();
        for (MemberImpl member : lsMembers) {
            final MemberImpl dummy = new MemberImpl(member.getAddress(), member.localMember(), member.getNodeType());
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
        if (listeners.size() > 0) {
            Set<Member> it = clusterMembers.keySet();
            // build a list of notifications but send them AFTER
            // removal
            for (final Member member : it) {
                if (!setNew.contains(member)) {
                    //node.executorManager.executeLocally(new Runnable() {
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
        for (Member member : setNew) {
            clusterMembers.put(member, member);
        }
        members.set(setNew);
        distances.clear();
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

    public long getClusterTime() {
        return System.currentTimeMillis() + ((clusterTimeDiff == Long.MAX_VALUE) ? 0 : clusterTimeDiff);
    }

    public void setMasterTime(long masterTime) {
        long diff = masterTime - System.currentTimeMillis();
        if (Math.abs(diff) < Math.abs(clusterTimeDiff)) {
            this.clusterTimeDiff = diff;
        }
    }

    public long getClusterTimeFor(long localTime) {
        return localTime + ((clusterTimeDiff == Long.MAX_VALUE) ? 0 : clusterTimeDiff);
    }
}
