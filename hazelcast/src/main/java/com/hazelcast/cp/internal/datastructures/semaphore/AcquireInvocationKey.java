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

package com.hazelcast.cp.internal.datastructures.semaphore;

import com.hazelcast.cp.internal.datastructures.spi.blocking.WaitKey;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.UUID;

import static com.hazelcast.cp.internal.util.UUIDSerializationUtil.readUUID;
import static com.hazelcast.cp.internal.util.UUIDSerializationUtil.writeUUID;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * Represents acquire() invocation of a semaphore endpoint.
 * A SemaphoreInvocationKey either holds some permits or resides
 * in the wait queue. Combination of a session id and a thread id a
 * single-threaded unique entity. When it sends a request X, it can either
 * retry this request X, or send a new request Y. After it sends request Y,
 * it will not retry request X anymore.
 */
public class AcquireInvocationKey implements WaitKey, IdentifiedDataSerializable {

    private long commitIndex;
    private SemaphoreEndpoint endpoint;
    private UUID invocationUid;
    private int permits;

    AcquireInvocationKey() {
    }

    AcquireInvocationKey(long commitIndex, SemaphoreEndpoint endpoint, UUID invocationUid, int permits) {
        checkNotNull(endpoint);
        checkNotNull(invocationUid);
        checkTrue(permits > 0, "permits must be positive");
        this.commitIndex = commitIndex;
        this.endpoint = endpoint;
        this.invocationUid = invocationUid;
        this.permits = permits;
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

    public SemaphoreEndpoint endpoint() {
        return endpoint;
    }

    public int permits() {
        return permits;
    }

    boolean isDifferentInvocationOf(SemaphoreEndpoint endpoint, UUID invocationUid) {
        return endpoint().equals(endpoint) && !invocationUid().equals(invocationUid);
    }

    @Override
    public int getFactoryId() {
        return RaftSemaphoreDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftSemaphoreDataSerializerHook.ACQUIRE_INVOCATION_KEY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(commitIndex);
        out.writeObject(endpoint);
        writeUUID(out, invocationUid);
        out.writeInt(permits);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        commitIndex = in.readLong();
        endpoint = in.readObject();
        invocationUid = readUUID(in);
        permits = in.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AcquireInvocationKey that = (AcquireInvocationKey) o;

        if (commitIndex != that.commitIndex) {
            return false;
        }
        if (permits != that.permits) {
            return false;
        }
        if (!endpoint.equals(that.endpoint)) {
            return false;
        }
        return invocationUid.equals(that.invocationUid);
    }

    @Override
    public int hashCode() {
        int result = (int) (commitIndex ^ (commitIndex >>> 32));
        result = 31 * result + endpoint.hashCode();
        result = 31 * result + invocationUid.hashCode();
        result = 31 * result + permits;
        return result;
    }

    @Override
    public String toString() {
        return "SemaphoreInvocationKey{" + "commitIndex=" + commitIndex + ", endpoint=" + endpoint
                + ", invocationUid=" + invocationUid + ", permits=" + permits + '}';
    }
}
