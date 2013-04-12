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

package com.hazelcast.management;

import com.hazelcast.cluster.MemberInfo;
import com.hazelcast.concurrent.lock.DistributedLock;
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.partition.MigrationInfo;
import com.hazelcast.partition.PartitionInfo;
import com.hazelcast.partition.PartitionRuntimeState;
import com.hazelcast.nio.Connection;

import java.io.IOException;
import java.util.*;

/**
 * @mdogan 5/8/12
 */
public class ClusterRuntimeState extends PartitionRuntimeState implements DataSerializable {

    private static final int LOCK_MAX_SIZE = 100;
    private int localMemberIndex;
    private Collection<ConnectionInfo> connectionInfos = new LinkedList<ConnectionInfo>();
    private List<LockInfo> lockInfos = new ArrayList<LockInfo>();
    private int lockTotalNum = 0;

    public ClusterRuntimeState() {
    }

    public ClusterRuntimeState(final Collection<Member> members, // !!! ordered !!!
                               final PartitionInfo[] partitions,
                               final MigrationInfo migrationInfo,  //TODO @msk ???
                               final Map<Address, Connection> connections,
                               final Collection<DistributedLock> distributedLocks) {
        super();
        final Map<Address, Integer> addressIndexes = new HashMap<Address, Integer>(members.size());
        int memberIndex = 0;
        for (Member member : members) {
            MemberImpl memberImpl = (MemberImpl) member;
            addMemberInfo(new MemberInfo(memberImpl.getAddress(), member.getUuid()),
                    addressIndexes, memberIndex);
            if (!member.localMember()) {
                final Connection conn = connections.get(memberImpl.getAddress());
                ConnectionInfo connectionInfo;
                if (conn != null) {
                    connectionInfo = new ConnectionInfo(memberIndex, conn.live(),
                            conn.lastReadTime(), conn.lastWriteTime());
                } else {
                    connectionInfo = new ConnectionInfo(memberIndex, false, 0L, 0L);
                }
                connectionInfos.add(connectionInfo);
            } else {
                localMemberIndex = memberIndex;
            }
            memberIndex++;
        }
        setPartitions(partitions, addressIndexes);
        setLocks(distributedLocks, addressIndexes, members);
    }

    private void setLocks(final Collection<DistributedLock> distributedLocks, final Map<Address, Integer> addressIndexes, final Collection<Member> members) {
//        final long now = Clock.currentTimeMillis();
        Map<String, Address> uuidToAddress = new HashMap<String, Address>(members.size());
        for (Member member : members) {
            uuidToAddress.put(member.getUuid(), ((MemberImpl) member).getAddress());
        }

        for (DistributedLock distributedLock : distributedLocks) {
            if (distributedLock.isLocked()) {
                Integer index = addressIndexes.get(uuidToAddress.get(distributedLock.getOwner()));
                if (index == null) {
                    index = -1;
                }
                lockInfos.add(new LockInfo(distributedLock.getOwner(), String.valueOf(distributedLock.getKey()),
                        distributedLock.getAcquireTime(), index, distributedLock.getLockCount()));
            }
        }
        lockTotalNum = lockInfos.size();
        Collections.sort(lockInfos, new Comparator<LockInfo>() {
            public int compare(LockInfo o1, LockInfo o2) {
                int comp1 = Integer.valueOf(o2.getWaitingThreadCount()).compareTo(Integer.valueOf(o1.getWaitingThreadCount()));
                if (comp1 == 0)
                    return Long.valueOf(o1.getAcquireTime()).compareTo(Long.valueOf(o2.getAcquireTime()));
                else return comp1;
            }
        });
        lockInfos = lockInfos.subList(0, Math.min(LOCK_MAX_SIZE, lockInfos.size()));
    }

    public MemberInfo getMember(int index) {
        return members.get(index);
    }

    public Collection<ConnectionInfo> getConnectionInfos() {
        return connectionInfos;
    }

    public MemberInfo getLocalMember() {
        return members.get(localMemberIndex);
    }

    public Collection<LockInfo> getLockInfos() {
        return lockInfos;
    }

    public int getLockTotalNum() {
        return lockTotalNum;
    }

    @Override
    public void readData(final ObjectDataInput in) throws IOException {
        super.readData(in);
        localMemberIndex = in.readInt();
        lockTotalNum = in.readInt();

        final int connectionInfoSize = in.readInt();
        for (int i = 0; i < connectionInfoSize; i++) {
            ConnectionInfo connectionInfo = new ConnectionInfo();
            connectionInfo.readData(in);
            connectionInfos.add(connectionInfo);
        }
        int lockSize = in.readInt();
        for (int i = 0; i < lockSize; i++) {
            LockInfo lockInfo = new LockInfo();
            lockInfo.readData(in);
            lockInfos.add(lockInfo);
        }
    }

    @Override
    public void writeData(final ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeInt(localMemberIndex);
        out.writeInt(lockTotalNum);
        out.writeInt(connectionInfos.size());
        for (ConnectionInfo info : connectionInfos) {
            info.writeData(out);
        }
        int lockSize = lockInfos.size();
        out.writeInt(lockSize);
        for (LockInfo lockInfo : lockInfos) {
            lockInfo.writeData(out);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("ClusterRuntimeState");
        sb.append("{members=").append(members);
        sb.append(", localMember=").append(localMemberIndex);
//        sb.append(", migrationInfo=").append(migrationInfo);
        sb.append(", waitingLockCount=").append(lockInfos.size());
        sb.append('}');
        return sb.toString();
    }
}
