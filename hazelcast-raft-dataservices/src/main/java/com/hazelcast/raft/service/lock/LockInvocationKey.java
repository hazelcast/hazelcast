/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.raft.service.blocking.WaitKey;

import java.io.IOException;
import java.util.UUID;

/**
 * Represents lock() invocation of a LockEndpoint.
 * A LockInvocationKey either holds the lock or resides in the wait queue.
 */
public class LockInvocationKey implements WaitKey, IdentifiedDataSerializable {
    private String name;
    private LockEndpoint endpoint;
    private long commitIndex;
    private UUID invocationUid;

    public LockInvocationKey() {
    }

    LockInvocationKey(String name, LockEndpoint endpoint, long commitIndex, UUID invocationUid) {
        this.name = name;
        this.endpoint = endpoint;
        this.commitIndex = commitIndex;
        this.invocationUid = invocationUid;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public long commitIndex() {
        return commitIndex;
    }

    @Override
    public long sessionId() {
        return endpoint.sessionId();
    }

    LockEndpoint endpoint() {
        return endpoint;
    }

    UUID invocationUid() {
        return invocationUid;
    }

    @Override
    public int getFactoryId() {
        return RaftLockDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftLockDataSerializerHook.LOCK_INVOCATION_KEY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeObject(endpoint);
        out.writeLong(commitIndex);
        out.writeLong(invocationUid.getLeastSignificantBits());
        out.writeLong(invocationUid.getMostSignificantBits());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        endpoint = in.readObject();
        commitIndex = in.readLong();
        long least = in.readLong();
        long most = in.readLong();
        invocationUid = new UUID(most, least);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LockInvocationKey waitEntry = (LockInvocationKey) o;

        if (commitIndex != waitEntry.commitIndex) {
            return false;
        }
        if (!name.equals(waitEntry.name)) {
            return false;
        }
        if (!endpoint.equals(waitEntry.endpoint)) {
            return false;
        }
        return invocationUid.equals(waitEntry.invocationUid);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + endpoint.hashCode();
        result = 31 * result + (int) (commitIndex ^ (commitIndex >>> 32));
        result = 31 * result + invocationUid.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "LockInvocationKey{" + "name='" + name + '\'' + ", endpoint=" + endpoint + ", commitIndex=" + commitIndex
                + ", invocationUid=" + invocationUid + '}';
    }
}
