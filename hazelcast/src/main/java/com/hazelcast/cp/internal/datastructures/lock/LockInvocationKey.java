/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.UUID;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Represents lock() invocation of a LockEndpoint.
 * A LockInvocationKey either holds the lock or resides in the wait queue.
 */
public class LockInvocationKey extends WaitKey implements IdentifiedDataSerializable {

    private LockEndpoint endpoint;

    public LockInvocationKey() {
    }

    public LockInvocationKey(long commitIndex, UUID invocationUid, Address callerAddress, long callId, LockEndpoint endpoint) {
        super(commitIndex, invocationUid, callerAddress, callId);
        checkNotNull(endpoint);
        this.endpoint = endpoint;
    }

    @Override
    public long sessionId() {
        return endpoint.sessionId();
    }

    LockEndpoint endpoint() {
        return endpoint;
    }

    boolean isDifferentInvocationOf(LockEndpoint endpoint, UUID invocationUid) {
        return endpoint().equals(endpoint) && !invocationUid().equals(invocationUid);
    }

    @Override
    public int getFactoryId() {
        return LockDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return LockDataSerializerHook.LOCK_INVOCATION_KEY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeObject(endpoint);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        endpoint = in.readObject();
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
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
        if (!invocationUid.equals(that.invocationUid)) {
            return false;
        }
        if (!callerAddress.equals(that.callerAddress)) {
            return false;
        }
        if (callId != that.callId) {
            return false;
        }
        return endpoint.equals(that.endpoint);
    }

    @Override
    public int hashCode() {
        int result = (int) (commitIndex ^ (commitIndex >>> 32));
        result = 31 * result + invocationUid.hashCode();
        result = 31 * result + callerAddress.hashCode();
        result = 31 * result + (int) (callId ^ (callId >>> 32));
        result = 31 * result + endpoint.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "LockInvocationKey{" + "endpoint=" + endpoint + ", commitIndex=" + commitIndex + ", invocationUid=" + invocationUid
                + ", callerAddress=" + callerAddress + ", callId=" + callId + '}';
    }
}
