/*
 *  Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.service.lock;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * TODO: Javadoc Pending...
 */
public class RaftLockSnapshot implements IdentifiedDataSerializable {

    private RaftGroupId groupId;
    private String name;
    private LockInvocationKey owner;
    private int lockCount;
    private UUID refUid;
    private List<LockInvocationKey> waitEntries;

    public RaftLockSnapshot() {
    }

    RaftLockSnapshot(RaftGroupId groupId, String name, LockInvocationKey owner, int lockCount, UUID refUid,
                            List<LockInvocationKey> waitEntries) {
        this.groupId = groupId;
        this.name = name;
        this.owner = owner;
        this.lockCount = lockCount;
        this.refUid = refUid;
        this.waitEntries = new ArrayList<LockInvocationKey>(waitEntries);
    }

    RaftGroupId getGroupId() {
        return groupId;
    }

    String getName() {
        return name;
    }

    LockInvocationKey getOwner() {
        return owner;
    }

    int getLockCount() {
        return lockCount;
    }

    UUID getRefUid() {
        return refUid;
    }

    List<LockInvocationKey> getWaitEntries() {
        return waitEntries;
    }

    @Override
    public int getFactoryId() {
        return RaftLockDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftLockDataSerializerHook.RAFT_LOCK_SNAPSHOT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(groupId);
        out.writeUTF(name);
        boolean hasOwner = (owner != null);
        out.writeBoolean(hasOwner);
        if (hasOwner) {
            out.writeObject(owner);
        }
        out.writeInt(lockCount);
        boolean hasRefUid = (refUid != null);
        out.writeBoolean(hasRefUid);
        if (hasRefUid) {
            out.writeLong(refUid.getLeastSignificantBits());
            out.writeLong(refUid.getMostSignificantBits());
        }
        out.writeInt(waitEntries.size());
        for (LockInvocationKey key : waitEntries) {
            out.writeObject(key);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        groupId = in.readObject();
        name = in.readUTF();
        boolean hasOwner = in.readBoolean();
        if (hasOwner) {
            owner = in.readObject();
        }
        lockCount = in.readInt();
        boolean hasRefUid = in.readBoolean();
        if (hasRefUid) {
            long least = in.readLong();
            long most = in.readLong();
            refUid = new UUID(most, least);
        }
        int count = in.readInt();
        waitEntries = new ArrayList<LockInvocationKey>();
        for (int i = 0; i < count; i++)  {
            LockInvocationKey key = in.readObject();
            waitEntries.add(key);
        }
    }
}
