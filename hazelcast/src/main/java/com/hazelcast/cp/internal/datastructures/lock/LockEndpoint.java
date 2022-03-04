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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;
import static com.hazelcast.internal.util.Preconditions.checkTrue;

/**
 * LockEndpoint represents a thread in a Raft client. It is a combination of
 * a session id and a thread id. A LockEndpoint is a single-threaded unique
 * entity. When it sends a request X, it can either retry this request X,
 * or send a new request Y. After it sends request Y, it will not retry
 * request X anymore.
 */
public class LockEndpoint implements IdentifiedDataSerializable {
    private long sessionId;
    private long threadId;

    public LockEndpoint() {
    }

    public LockEndpoint(long sessionId, long threadId) {
        checkTrue(sessionId != NO_SESSION_ID, "a session id must be provided");
        this.sessionId = sessionId;
        this.threadId = threadId;
    }

    public long sessionId() {
        return sessionId;
    }

    public long threadId() {
        return threadId;
    }

    @Override
    public int getFactoryId() {
        return LockDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return LockDataSerializerHook.LOCK_ENDPOINT;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeLong(sessionId);
        out.writeLong(threadId);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        sessionId = in.readLong();
        threadId = in.readLong();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LockEndpoint)) {
            return false;
        }

        LockEndpoint that = (LockEndpoint) o;

        if (sessionId != that.sessionId) {
            return false;
        }
        return threadId == that.threadId;
    }

    @Override
    public int hashCode() {
        int result = (int) (sessionId ^ (sessionId >>> 32));
        result = 31 * result + (int) (threadId ^ (threadId >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "LockEndpoint{" + "sessionId=" + sessionId + ", threadId=" + threadId + '}';
    }
}
