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

package com.hazelcast.cp.internal.datastructures.countdownlatch;

import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.cp.internal.datastructures.spi.blocking.WaitKey;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;
import static com.hazelcast.cp.internal.util.UUIDSerializationUtil.readUUID;
import static com.hazelcast.cp.internal.util.UUIDSerializationUtil.writeUUID;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Represents a {@link ICountDownLatch#await(long, TimeUnit)}} invocation
 */
public class AwaitInvocationKey implements WaitKey, IdentifiedDataSerializable {

    private long commitIndex;
    private UUID invocationUid;

    AwaitInvocationKey() {
    }

    AwaitInvocationKey(long commitIndex, UUID invocationUid) {
        checkNotNull(invocationUid);
        this.commitIndex = commitIndex;
        this.invocationUid = invocationUid;
    }

    @Override
    public long sessionId() {
        return NO_SESSION_ID;
    }

    @Override
    public long commitIndex() {
        return commitIndex;
    }

    @Override
    public UUID invocationUid() {
        return invocationUid;
    }

    @Override
    public int getFactoryId() {
        return RaftCountDownLatchDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftCountDownLatchDataSerializerHook.AWAIT_INVOCATION_KEY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(commitIndex);
        writeUUID(out, invocationUid);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
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

        AwaitInvocationKey that = (AwaitInvocationKey) o;

        if (commitIndex != that.commitIndex) {
            return false;
        }
        return invocationUid.equals(that.invocationUid);
    }

    @Override
    public int hashCode() {
        int result = (int) (commitIndex ^ (commitIndex >>> 32));
        result = 31 * result + invocationUid.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "AwaitInvocationKey{" + "commitIndex=" + commitIndex + ", invocationUid=" + invocationUid + '}';
    }
}
