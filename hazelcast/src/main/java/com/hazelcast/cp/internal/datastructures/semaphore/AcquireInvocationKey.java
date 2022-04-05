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

package com.hazelcast.cp.internal.datastructures.semaphore;

import com.hazelcast.cp.internal.datastructures.spi.blocking.WaitKey;
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.UUID;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkTrue;

/**
 * Represents acquire() invocation of a semaphore endpoint.
 * A SemaphoreInvocationKey either holds some permits or resides
 * in the wait queue. Combination of a session id and a thread id a
 * single-threaded unique entity. When it sends a request X, it can either
 * retry this request X, or send a new request Y. After it sends request Y,
 * it will not retry request X anymore.
 */
public class AcquireInvocationKey extends WaitKey implements IdentifiedDataSerializable {

    private SemaphoreEndpoint endpoint;
    private int permits;

    AcquireInvocationKey() {
    }

    public AcquireInvocationKey(long commitIndex, UUID invocationUid, Address callerAddress, long callId,
                                SemaphoreEndpoint endpoint, int permits) {
        super(commitIndex, invocationUid, callerAddress, callId);
        checkNotNull(endpoint);
        checkTrue(permits > 0, "permits must be positive");
        this.endpoint = endpoint;
        this.permits = permits;
    }

    @Override
    public long sessionId() {
        return endpoint.sessionId();
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
        return SemaphoreDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SemaphoreDataSerializerHook.ACQUIRE_INVOCATION_KEY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeObject(endpoint);
        out.writeInt(permits);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        endpoint = in.readObject();
        permits = in.readInt();
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

        AcquireInvocationKey that = (AcquireInvocationKey) o;

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
        if (!endpoint.equals(that.endpoint)) {
            return false;
        }
        return permits == that.permits;
    }

    @Override
    public int hashCode() {
        int result = (int) (commitIndex ^ (commitIndex >>> 32));
        result = 31 * result + invocationUid.hashCode();
        result = 31 * result + callerAddress.hashCode();
        result = 31 * result + (int) (callId ^ (callId >>> 32));
        result = 31 * result + endpoint.hashCode();
        result = 31 * result + permits;
        return result;
    }

    @Override
    public String toString() {
        return "AcquireInvocationKey{" + "endpoint=" + endpoint + ", permits=" + permits + ", commitIndex=" + commitIndex
                + ", invocationUid=" + invocationUid + ", callerAddress=" + callerAddress + ", callId=" + callId + '}';
    }
}
