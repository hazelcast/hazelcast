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
import com.hazelcast.concurrent.lock.LockResource;
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.MigrationInfo;
import com.hazelcast.partition.PartitionRuntimeState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

//todo: cluster runtime should not extend PartitionRuntime but should compose it;
//favor composition over inheritance.
public class ClusterRuntimeState extends PartitionRuntimeState implements DataSerializable {

    private static final int LOCK_MAX_SIZE = 100;

    private int localMemberIndex;
    private Collection<ConnectionInfo> connectionInfos;
    private Collection<MigrationInfo> activeMigrations;
    private List<LockInfo> lockInfos;
    private int lockTotalNum = 0;

    public ClusterRuntimeState() {
    }

    public ClusterRuntimeState( Collection<Member> members, // !!! ordered !!!
                                InternalPartition[] partitions,
                                Collection<MigrationInfo> activeMigrations,
                                Map<Address, Connection> connections,
                                Collection<LockResource> locks) {
        this.activeMigrations = activeMigrations != null ? activeMigrations : Collections.<MigrationInfo>emptySet();
        lockInfos = new LinkedList<LockInfo>();
        connectionInfos = new LinkedList<ConnectionInfo>();
         Map<Address, Integer> addressIndexes = new HashMap<Address, Integer>(members.size());
        int memberIndex = 0;
        for (Member member : members) {
            MemberImpl memberImpl = (MemberImpl) member;
            MemberInfo memberInfo = new MemberInfo(memberImpl.getAddress(), member.getUuid());
            addMemberInfo(memberInfo, addressIndexes, memberIndex);
            if (!member.localMember()) {
                 Connection conn = connections.get(memberImpl.getAddress());
                ConnectionInfo connectionInfo;
                if (conn != null) {
                    connectionInfo = new ConnectionInfo(memberIndex, conn.live(), conn.lastReadTime(), conn.lastWriteTime());
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
        setLocks(locks, addressIndexes, members);
    }

    private void setLocks( Collection<LockResource> locks,  Map<Address, Integer> addressIndexes,  Collection<Member> members) {
        Map<String, Address> uuidToAddress = new HashMap<String, Address>(members.size());
        for (Member member : members) {
            uuidToAddress.put(member.getUuid(), ((MemberImpl) member).getAddress());
        }

        for (LockResource lock : locks) {
            if (lock.isLocked()) {
                Integer index = addressIndexes.get(uuidToAddress.get(lock.getOwner()));
                if (index == null) {
                    index = -1;
                }
                lockInfos.add(new LockInfo(lock.getOwner(), String.valueOf(lock.getKey()),
                        lock.getAcquireTime(), index, lock.getLockCount()));
            }
        }
        lockTotalNum = lockInfos.size();
        Collections.sort(lockInfos, new Comparator<LockInfo>() {
            public int compare(LockInfo o1, LockInfo o2) {
                int comp1 = Integer.valueOf(o2.getWaitingThreadCount()).compareTo(o1.getWaitingThreadCount());
                if (comp1 == 0)
                    return Long.valueOf(o1.getAcquireTime()).compareTo(o2.getAcquireTime());
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

    public Collection<MigrationInfo> getActiveMigrations() {
        return activeMigrations;
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
    public void readData( ObjectDataInput in) throws IOException {
        super.readData(in);
        localMemberIndex = in.readInt();
        lockTotalNum = in.readInt();

        int connectionInfoSize = in.readInt();
        connectionInfos = new ArrayList<ConnectionInfo>(connectionInfoSize);
        for (int i = 0; i < connectionInfoSize; i++) {
            ConnectionInfo connectionInfo = new ConnectionInfo();
            connectionInfo.readData(in);
            connectionInfos.add(connectionInfo);
        }

        int migrationsSize = in.readInt();
        activeMigrations = new ArrayList<MigrationInfo>(migrationsSize);
        for (int i = 0; i < migrationsSize; i++) {
            MigrationInfo migrationInfo = new MigrationInfo();
            migrationInfo.readData(in);
            activeMigrations.add(migrationInfo);
        }

        int lockSize = in.readInt();
        lockInfos = new ArrayList<LockInfo>(lockSize);
        for (int i = 0; i < lockSize; i++) {
            LockInfo lockInfo = new LockInfo();
            lockInfo.readData(in);
            lockInfos.add(lockInfo);
        }
    }

    @Override
    public void writeData( ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeInt(localMemberIndex);
        out.writeInt(lockTotalNum);

        int connectionInfoSize = connectionInfos != null ? connectionInfos.size() : 0;
        out.writeInt(connectionInfoSize);
        if (connectionInfoSize > 0) {
            for (ConnectionInfo info : connectionInfos) {
                info.writeData(out);
            }
        }

        int migrationsSize = activeMigrations != null ? activeMigrations.size() : 0;
        out.writeInt(migrationsSize);
        if (migrationsSize > 0) {
            for (MigrationInfo migrationInfo : activeMigrations) {
                migrationInfo.writeData(out);
            }
        }

        int lockSize = lockInfos != null ? lockInfos.size() : 0;
        out.writeInt(lockSize);
        if (lockSize > 0) {
            for (LockInfo lockInfo : lockInfos) {
                lockInfo.writeData(out);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ClusterRuntimeState");
        sb.append("{members=").append(members);
        sb.append(", localMember=").append(localMemberIndex);
        sb.append(", activeMigrations=").append(activeMigrations);
        sb.append(", waitingLockCount=").append(lockInfos.size());
        sb.append('}');
        return sb.toString();
    }
}
