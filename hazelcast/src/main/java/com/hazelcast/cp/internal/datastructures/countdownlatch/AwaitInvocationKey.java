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

package com.hazelcast.cp.internal.datastructures.countdownlatch;

import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.cp.internal.datastructures.spi.blocking.WaitKey;
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;

/**
 * Represents a {@link ICountDownLatch#await(long, TimeUnit)}} invocation
 */
public class AwaitInvocationKey extends WaitKey implements IdentifiedDataSerializable {

    AwaitInvocationKey() {
    }

    public AwaitInvocationKey(long commitIndex, UUID invocationUid, Address callerAddress, long callId) {
        super(commitIndex, invocationUid, callerAddress, callId);
    }

    @Override
    public long sessionId() {
        return NO_SESSION_ID;
    }

    @Override
    public int getFactoryId() {
        return CountDownLatchDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CountDownLatchDataSerializerHook.AWAIT_INVOCATION_KEY;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AwaitInvocationKey that = (AwaitInvocationKey) o;

        if (commitIndex != that.commitIndex) {
            return false;
        }
        if (!invocationUid.equals(that.invocationUid)) {
            return false;
        }
        if (!callerAddress.equals(that.callerAddress)) {
            return false;
        }
        return callId == that.callId;
    }

    @Override
    public int hashCode() {
        int result = (int) (commitIndex ^ (commitIndex >>> 32));
        result = 31 * result + invocationUid.hashCode();
        result = 31 * result + callerAddress.hashCode();
        result = 31 * result + (int) (callId ^ (callId >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "AwaitInvocationKey{" + "commitIndex=" + commitIndex + ", invocationUid=" + invocationUid + ", callerAddress="
                + callerAddress + ", callId=" + callId + '}';
    }
}
