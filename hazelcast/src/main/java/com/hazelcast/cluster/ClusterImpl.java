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
import java.util.concurrent.atomic.AtomicReference;

public class ClusterImpl implements Cluster {

    final AtomicReference<Set<MembershipListener>> listeners = new AtomicReference<Set<MembershipListener>>();
    final AtomicReference<Set<Member>> members = new AtomicReference<Set<Member>>();
    final AtomicReference<Member> localMember = new AtomicReference<Member>();
    final Map<Member, Member> clusterMembers = new ConcurrentHashMap<Member, Member>();
    volatile long clusterTimeDiff = Long.MAX_VALUE;
    final Node node;

    public ClusterImpl(Node node) {
        this.node = node;
    }

    public void setMembers(List<MemberImpl> lsMembers) {
        final Set<MembershipListener> listenerSet = listeners.get();
        Set<Member> setNew = new LinkedHashSet<Member>(lsMembers.size());
        ArrayList<Runnable> notifications = new ArrayList<Runnable>();
        for (MemberImpl member : lsMembers) {
            final MemberImpl dummy = new MemberImpl(member.getAddress(), member.localMember(), member.getNodeType());
            Member clusterMember = clusterMembers.get(dummy);
            if (clusterMember == null) {
                clusterMember = dummy;
                if (listenerSet != null && listenerSet.size() > 0) {
                    notifications.add(new Runnable() {
                        public void run() {
                            MembershipEvent membershipEvent = new MembershipEvent(ClusterImpl.this,
                                    dummy, MembershipEvent.MEMBER_ADDED);
                            for (MembershipListener listener : listenerSet) {
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
        if (listenerSet != null && listenerSet.size() > 0) {
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
                            for (MembershipListener listener : listenerSet) {
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
        // send notifications now
        for (Runnable notification : notifications) {
            node.executorManager.executeLocally(notification);
        }
    }

    public void addMembershipListener(MembershipListener listener) {
        Set<MembershipListener> oldListeners = listeners.get();
        int size = (oldListeners == null) ? 1 : oldListeners.size() + 1;
        Set<MembershipListener> newListeners = new LinkedHashSet<MembershipListener>(size);
        if (oldListeners != null) {
            for (MembershipListener existingListener : oldListeners) {
                newListeners.add(existingListener);
            }
        }
        newListeners.add(listener);
        listeners.set(newListeners);
    }

    public void removeMembershipListener(MembershipListener listener) {
        Set<MembershipListener> oldListeners = listeners.get();
        if (oldListeners == null || oldListeners.size() == 0)
            return;
        int size = oldListeners.size() - 1;
        Set<MembershipListener> newListeners = new LinkedHashSet<MembershipListener>(size);
        for (MembershipListener existingListener : oldListeners) {
            if (!existingListener.equals(listener)) {
                newListeners.add(existingListener);
            }
        }
        listeners.set(newListeners);
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
