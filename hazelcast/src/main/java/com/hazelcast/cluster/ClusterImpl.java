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
    final Map<Member, Integer> distancesWithoutSuper = new ConcurrentHashMap<Member, Integer>();
    @SuppressWarnings("VolatileLongOrDoubleField")
    volatile long clusterTimeDiff = Long.MAX_VALUE;
    final Node node;

    public ClusterImpl(Node node, MemberImpl thisMember) {
        this.node = node;
        this.setMembers(Arrays.asList(thisMember));
    }

    public void reset() {
        clusterMembers.clear();
        distancesWithoutSuper.clear();
        distances.clear();
        members.set(null);
        this.setMembers(Arrays.asList((MemberImpl) localMember.get()));
    }

    public int getDistanceFrom(Member member, boolean skipSuper) {
        if (member.localMember()) {
            return 0;
        }
        if (skipSuper) {
            Integer distance = distancesWithoutSuper.get(member);
            if (distance != null) {
                return distance;
            }
        } else {
            Integer distance = distances.get(member);
            if (distance != null) {
                return distance;
            }
        }
        calculateDistances();
        if (skipSuper) {
            Integer distance = distancesWithoutSuper.get(member);
            return (distance == null) ? -1 : distance;
        } else {
            Integer distance = distances.get(member);
            return (distance == null) ? -1 : distance;
        }
    }

    public int getDistanceFrom(Member member) {
        if (member.localMember()) {
            return 0;
        }
        Integer distance = distances.get(member);
        if (distance != null) {
            return distance;
        }
        calculateDistances();
        Integer d = distances.get(member);
        return (d == null) ? -1 : d;
    }

    private void calculateDistances() {
        List<Member> currentMembers = new ArrayList<Member>(members.get());
        for (int i = 0; i < currentMembers.size(); i++) {
            Member member = currentMembers.get(i);
            if (!member.localMember()) {
                distances.put(member, calculateDistance(currentMembers, member, localMember.get(), false));
                distancesWithoutSuper.put(member, calculateDistance(currentMembers, member, localMember.get(), true));
            }
        }
    }

    public static int calculateDistance(List<Member> members, Member from, Member to, boolean skipSuper) {
        int indexFrom = members.indexOf(from);
        int indexTo = members.indexOf(to);
        if (indexTo < indexFrom) {
            indexTo += members.size();
        }
        if (skipSuper) {
            int d = 0;
            for (int i = indexFrom; i < indexTo; i++) {
                int a = i % members.size();
                Member member = members.get(a);
                if (!member.isSuperClient()) {
                    d++;
                }
            }
            return d;
        } else {
            return indexTo - indexFrom;
        }
    }

    public void setMembers(List<MemberImpl> lsMembers) {
        Set<Member> setNew = new LinkedHashSet<Member>(lsMembers.size());
        ArrayList<Runnable> notifications = new ArrayList<Runnable>();
        for (MemberImpl member : lsMembers) {
            if (member != null) {
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
        members.set(Collections.unmodifiableSet(setNew));
        distances.clear();
        distancesWithoutSuper.clear();
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
