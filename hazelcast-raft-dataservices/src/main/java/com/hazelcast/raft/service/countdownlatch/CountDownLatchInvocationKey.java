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

package com.hazelcast.raft.service.countdownlatch;

import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.service.blocking.WaitKey;

import java.io.IOException;

import static com.hazelcast.raft.service.session.AbstractSessionManager.NO_SESSION_ID;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Represents a {@link ICountDownLatch#countDown()} invocation
 */
public class CountDownLatchInvocationKey implements WaitKey, IdentifiedDataSerializable {

    private String name;
    private long commitIndex;

    CountDownLatchInvocationKey() {
    }

    CountDownLatchInvocationKey(String name, long commitIndex) {
        checkNotNull(name);
        this.name = name;
        this.commitIndex = commitIndex;
    }

    @Override
    public String name() {
        return name;
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
    public int getFactoryId() {
        return RaftCountDownLatchDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftCountDownLatchDataSerializerHook.COUNT_DOWN_LATCH_INVOCATION_KEY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeLong(commitIndex);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        commitIndex = in.readLong();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CountDownLatchInvocationKey that = (CountDownLatchInvocationKey) o;

        if (commitIndex != that.commitIndex) {
            return false;
        }
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + (int) (commitIndex ^ (commitIndex >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "CountDownLatchInvocationKey{" + "name='" + name + '\'' + ", commitIndex=" + commitIndex + '}';
    }

}
