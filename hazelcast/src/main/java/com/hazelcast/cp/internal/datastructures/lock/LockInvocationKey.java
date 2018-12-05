/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.lock;

import com.hazelcast.cp.internal.datastructures.spi.blocking.WaitKey;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.UUID;

import static com.hazelcast.cp.internal.util.UUIDSerializationUtil.readUUID;
import static com.hazelcast.cp.internal.util.UUIDSerializationUtil.writeUUID;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Represents lock() invocation of a LockEndpoint.
 * A LockInvocationKey either holds the lock or resides in the wait queue.
 */
public class LockInvocationKey implements WaitKey, IdentifiedDataSerializable {

    private LockEndpoint endpoint;
    private long commitIndex;
    private UUID invocationUid;

    public LockInvocationKey() {
    }

    LockInvocationKey(LockEndpoint endpoint, long commitIndex, UUID invocationUid) {
        checkNotNull(endpoint);
        checkNotNull(invocationUid);
        this.endpoint = endpoint;
        this.commitIndex = commitIndex;
        this.invocationUid = invocationUid;
    }

    @Override
    public long commitIndex() {
        return commitIndex;
    }

    @Override
    public long sessionId() {
        return endpoint.sessionId();
    }

    @Override
    public UUID invocationUid() {
        return invocationUid;
    }

    LockEndpoint endpoint() {
        return endpoint;
    }

    boolean isDifferentInvocationOf(LockEndpoint endpoint, UUID invocationUid) {
        return endpoint().equals(endpoint) && !invocationUid().equals(invocationUid);
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
        out.writeObject(endpoint);
        out.writeLong(commitIndex);
        writeUUID(out, invocationUid);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        endpoint = in.readObject();
        commitIndex = in.readLong();
        invocationUid = readUUID(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LockInvocationKey that = (LockInvocationKey) o;

        if (commitIndex != that.commitIndex) {
            return false;
        }
        if (!endpoint.equals(that.endpoint)) {
            return false;
        }
        return invocationUid.equals(that.invocationUid);
    }

    @Override
    public int hashCode() {
        int result = endpoint.hashCode();
        result = 31 * result + (int) (commitIndex ^ (commitIndex >>> 32));
        result = 31 * result + invocationUid.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "LockInvocationKey{" + "endpoint=" + endpoint + ", commitIndex=" + commitIndex + ", invocationUid=" + invocationUid
                + '}';
    }
}
