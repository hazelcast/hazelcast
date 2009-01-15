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

package com.hazelcast.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import com.hazelcast.core.Cluster;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.DataSerializable;

public class ClusterImpl implements Cluster {

	AtomicReference<Set> refListeners = new AtomicReference<Set>();

	AtomicReference<Set> refMembers = new AtomicReference<Set>();

	AtomicReference<ClusterMember> refLocalMember = new AtomicReference<ClusterMember>();

	Map<ClusterMember, ClusterMember> mapClusterMembers = new ConcurrentHashMap<ClusterMember, ClusterMember>();

	long clusterTimeDiff = 0;

	ClusterImpl() {

	}

	public void setMembers(List<MemberImpl> lsMembers) {
		final Set<MembershipListener> setListeners = refListeners.get();
		Set<ClusterMember> setNew = new LinkedHashSet<ClusterMember>(lsMembers.size());
		for (MemberImpl member : lsMembers) {
			final ClusterMember dummy = new ClusterMember(member.getAddress(), member.localMember());
			ClusterMember clusterMember = mapClusterMembers.get(dummy);
			if (clusterMember == null) {
				clusterMember = dummy;
				if (setListeners != null && setListeners.size() > 0) {
					new Thread(new Runnable() {
						public void run() {
							MembershipEvent membershipEvent = new MembershipEvent(ClusterImpl.this,
									dummy, MembershipEvent.MEMBER_ADDED);
							for (MembershipListener listener : setListeners) {
								listener.memberAdded(membershipEvent);
							}
						}
					}).start();
				}
			}
			if (clusterMember.localMember()) {
				refLocalMember.set(clusterMember);
			}
			setNew.add(clusterMember);
		}
		if (setListeners != null && setListeners.size() > 0) {
			Set<ClusterMember> it = mapClusterMembers.keySet();
			for (final ClusterMember cm : it) {
				if (!setNew.contains(cm)) {
					new Thread(new Runnable() {
						public void run() {
							MembershipEvent membershipEvent = new MembershipEvent(ClusterImpl.this,
									cm, MembershipEvent.MEMBER_REMOVED);
							for (MembershipListener listener : setListeners) {
								listener.memberRemoved(membershipEvent);
							}
						}
					}).start();
				}
			}
		}

		mapClusterMembers.clear();
		for (ClusterMember cm : setNew) {
			mapClusterMembers.put(cm, cm);
		}
		refMembers.set(setNew);
	}

	public void addMembershipListener(MembershipListener listener) {
		Set<MembershipListener> setOldListeners = refListeners.get();
		int size = (setOldListeners == null) ? 1 : setOldListeners.size() + 1;
		Set<MembershipListener> setNewListeners = new LinkedHashSet<MembershipListener>(size);
		if (setOldListeners != null) {
			for (MembershipListener existingListener : setOldListeners) {
				setNewListeners.add(existingListener);
			}
		}
		setNewListeners.add(listener);
		refListeners.set(setNewListeners);
	}

	public void removeMembershipListener(MembershipListener listener) {
		Set<MembershipListener> setOldListeners = refListeners.get();
		if (setOldListeners == null || setOldListeners.size() == 0)
			return;
		int size = setOldListeners.size() - 1;
		Set<MembershipListener> setNewListeners = new LinkedHashSet<MembershipListener>(size);
		for (MembershipListener existingListener : setOldListeners) {
			if (existingListener.equals(listener))
				setNewListeners.add(existingListener);
		}
		refListeners.set(setNewListeners);
	}

	public Member getLocalMember() {
		return refLocalMember.get();
	}

	public Set<Member> getMembers() {
		return refMembers.get();
	}

	@Override
	public String toString() {
		Set<Member> members = getMembers();
		StringBuffer sb = new StringBuffer("Cluster [");
		if (members != null) {
			sb.append(members.size());
			sb.append("] {");
			for (Member member : members) {
				sb.append("\n\t" + member);
				if (member.localMember()) {
					sb.append(" local");
				}
			}
		}
		sb.append("\n}\n");
		return sb.toString();
	}

	public static class ClusterMember implements Member, DataSerializable {

		Address address = null;

		boolean localMember = false;

		public ClusterMember() {
			super();
		}

		public ClusterMember(Address address, boolean localMember) {
			super();
			this.address = address;
			this.localMember = localMember;
		}

		public InetAddress getInetAddress() {
			try {
				return address.getInetAddress();
			} catch (UnknownHostException e) {
				return null;
			}
		}

		public Address getAddress() {
			return address;
		}

		public int getPort() {
			return address.getPort();
		}

		public boolean localMember() {
			return localMember;
		}

		public void readData(DataInput in) throws IOException {
			address = new Address();
			address.readData(in);
			localMember = Node.get().getThisAddress().equals(address);
		}

		public void writeData(DataOutput out) throws IOException {
			address.writeData(out);
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder("Member [");
			sb.append(address.getHost());
			sb.append(":");
			sb.append(address.getPort());
			sb.append("] " + localMember);
			return sb.toString();
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
