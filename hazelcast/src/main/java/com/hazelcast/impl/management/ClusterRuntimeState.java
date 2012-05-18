/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl.management;

import com.hazelcast.cluster.MemberInfo;
import com.hazelcast.core.Member;
import com.hazelcast.impl.MemberImpl;
import com.hazelcast.impl.Record;
import com.hazelcast.impl.partition.MigratingPartition;
import com.hazelcast.impl.partition.PartitionInfo;
import com.hazelcast.impl.partition.PartitionRuntimeState;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.util.Clock;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * @mdogan 5/8/12
 */
public class ClusterRuntimeState extends PartitionRuntimeState implements DataSerializable {

    private int localMemberIndex;
    private Collection<ConnectionInfo> connectionInfos = new LinkedList<ConnectionInfo>();
    private List<LockInfo> lockInfos = new ArrayList<LockInfo>();
    private int lockTotalNum = 0;
    private MigratingPartition migratingPartition;
    private final int LOCK_MAX_SIZE = 100;

    public ClusterRuntimeState() {
    }

    public ClusterRuntimeState(final Collection<Member> members, // !!! ordered !!!
                               final PartitionInfo[] partitions,
                               final MigratingPartition migratingPartition,
                               final Map<Address, Connection> connections,
                               final Collection<Record> lockedRecords) {
        super();

        final Map<Address, Integer> addressIndexes = new HashMap<Address, Integer>(members.size());
        int memberIndex = 0;
        for (Member member : members) {
            MemberImpl memberImpl = (MemberImpl) member;
            addMemberInfo(new MemberInfo(memberImpl.getAddress(), memberImpl.getNodeType(), member.getUuid()),
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
        setLocks(lockedRecords, addressIndexes);
        this.migratingPartition = migratingPartition;
    }

    private void setLocks(final Collection<Record> lockedRecords, final Map<Address, Integer> addressIndexes) {
        final long now = Clock.currentTimeMillis();
        long min = Long.MAX_VALUE;
        for (Record record : lockedRecords) {
            if (record.isActive() && record.isValid(now) && record.isLocked()) {
                Address owner = record.getLockAddress();
                Integer index = addressIndexes.get(owner);
                if (index == null) {
                    index = -1;
                }
                lockInfos.add(new LockInfo(record.getName(), String.valueOf(record.getKey()),
                                           record.getLockAcquireTime(), index, record.getScheduledActionCount()));
            }
        }
        lockTotalNum = lockInfos.size();
        Collections.sort(lockInfos, new Comparator<LockInfo>() {
            public int compare(LockInfo o1, LockInfo o2) {
                return Long.valueOf(o1.getAcquireTime()).compareTo(Long.valueOf(o2.getAcquireTime()));
            }
        });
        lockInfos = lockInfos.subList(0,Math.min(LOCK_MAX_SIZE,lockInfos.size()));
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

    public MigratingPartition getMigratingPartition() {
        return migratingPartition;
    }

    @Override
    public void readData(final DataInput in) throws IOException {
        localMemberIndex = in.readInt();
        lockTotalNum = in.readInt();
        boolean hasMigratingPartition = in.readBoolean();
        if (hasMigratingPartition) {
            migratingPartition = new MigratingPartition();
            migratingPartition.readData(in);
        }
        super.readData(in);
        int size = members.size() - 1; // do not count local member
        for (int i = 0; i < size; i++) {
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
    public void writeData(final DataOutput out) throws IOException {
        out.writeInt(localMemberIndex);
        out.writeInt(lockTotalNum);
        boolean hasMigratingPartition = migratingPartition != null;
        out.writeBoolean(hasMigratingPartition);
        if (hasMigratingPartition) {
            migratingPartition.writeData(out);
        }
        super.writeData(out);
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
        sb.append(", migratingPartition=").append(migratingPartition);
        sb.append(", waitingLockCount=").append(lockInfos.size());
        sb.append('}');
        return sb.toString();
    }
}
