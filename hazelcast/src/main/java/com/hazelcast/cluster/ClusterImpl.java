/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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
import com.hazelcast.impl.FactoryImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class ClusterImpl implements Cluster {

    final AtomicReference<Set<MembershipListener>> listeners = new AtomicReference<Set<MembershipListener>>();
    final AtomicReference<Set<Member>> members = new AtomicReference<Set<Member>>();
    final AtomicReference<Member> localMember = new AtomicReference<Member>();
    final Map<Member, Member> clusterMembers = new ConcurrentHashMap<Member, Member>();
    long clusterTimeDiff = 0;
    final Node node;

    public ClusterImpl(Node node) {
        this.node = node;
    }

    public void setMembers(List<MemberImpl> lsMembers) {
        final Set<MembershipListener> listenerSet = listeners.get();
        Set<Member> setNew = new LinkedHashSet<Member>(lsMembers.size());
        for (MemberImpl member : lsMembers) {
            final ClusterMember dummy = new ClusterMember(node.factory.getName(), member.getAddress(), member.localMember(), member.getNodeType());
            Member clusterMember = clusterMembers.get(dummy);
            if (clusterMember == null) {
                clusterMember = dummy; 
                if (listenerSet != null && listenerSet.size() > 0) {
                    node.executorManager.executeLocaly(new Runnable() {
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
            for (final Member cm : it) {
                if (!setNew.contains(cm)) {
                    node.executorManager.executeLocaly(new Runnable() {
                        public void run() {
                            MembershipEvent membershipEvent = new MembershipEvent(ClusterImpl.this,
                                    cm, MembershipEvent.MEMBER_REMOVED);
                            for (MembershipListener listener : listenerSet) {
                                listener.memberRemoved(membershipEvent);
                            }
                        }
                    });
                }
            }
        }

        clusterMembers.clear();
        for (Member cm : setNew) {
            clusterMembers.put(cm, cm);
        }
        members.set(setNew);
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

    public static class ClusterMember extends MemberImpl implements Member, DataSerializable {
        String factoryName;

        public ClusterMember() {
        }

        public ClusterMember(String factoryName, Address address, boolean localMember, Node.NodeType nodeType) {
            super(address, localMember, nodeType);
            this.factoryName = factoryName;
        }

        public void readData(DataInput in) throws IOException {
            address = new Address();
            address.readData(in);
            nodeType = Node.NodeType.create(in.readInt());
            factoryName = in.readUTF();
            Node node = FactoryImpl.getFactoryImpl(factoryName).node;
            localMember = node.getThisAddress().equals(address);
        }

        public void writeData(DataOutput out) throws IOException {
            address.writeData(out);
            out.writeInt(nodeType.getValue());

        }

        @Override
        public int hashCode() {
            final int PRIME = 31;
            int result = 1;
            result = PRIME * result + ((address == null) ? 0 : address.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            final ClusterMember other = (ClusterMember) obj;
            if (address == null) {
                if (other.address != null)
                    return false;
            } else if (!address.equals(other.address))
                return false;
            return true;
        }

    }

    public void setClusterTimeDiff(long clusterTimeDiff) {
        this.clusterTimeDiff = clusterTimeDiff;
    }

    public long getClusterTime() {
        return System.currentTimeMillis() + clusterTimeDiff;
    }

}
