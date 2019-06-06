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

package com.hazelcast.cp.internal.datastructures.spi.blocking;

import com.hazelcast.cp.internal.datastructures.RaftDataServiceDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * Contains all wait keys for an invocation uid within the same session,
 * including the first invocation and its retries
 *
 * @param <W> type of the wait key
 */
public class WaitKeyContainer<W extends WaitKey> implements IdentifiedDataSerializable {

    /**
     * Wait key of the first invocation.
     */
    private W key;

    /**
     * All wait keys, including the first invocation and its retries.
     */
    private List<W> keys;

    public WaitKeyContainer() {
    }

    WaitKeyContainer(W key) {
        this.key = key;
    }

    public W key() {
        return key;
    }

    public Collection<W> retries() {
        return keys != null ? keys.subList(1, keys.size()) : Collections.emptyList();
    }

    public Collection<W> keyAndRetries() {
        if (keys == null) {
            return Collections.singletonList(key);
        }
        return keys;
    }

    public int retryCount() {
        return keys != null ? keys.size() - 1 : 0;
    }

    public long sessionId() {
        return key.sessionId();
    }

    public UUID invocationUid() {
        return key.invocationUid();
    }

    void addRetry(W retry) {
        checkTrue(sessionId() == key.sessionId(), key + " and its retry: " + retry
                + " has different session ids!");
        checkTrue(key.invocationUid().equals(retry.invocationUid()), key + " and its retry: " + retry
                + " has different invocation uids!");

        if (keys == null) {
            keys = new ArrayList<>(3);
            keys.add(key);
        }
        keys.add(retry);
    }

    @Override
    public int getFactoryId() {
        return RaftDataServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftDataServiceDataSerializerHook.WAIT_KEY_CONTAINER;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(key);
        Collection<W> retries = retries();
        out.writeInt(retries.size());
        for (W retry : retries) {
            out.writeObject(retry);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        key = in.readObject();
        int retryCount = in.readInt();
        if (retryCount > 0) {
            keys = new ArrayList<>(retryCount + 1);
            keys.add(key);
            for (int i = 0; i < retryCount; i++) {
                W retry = in.readObject();
                keys.add(retry);
            }
        }
    }

    public String toString() {
        return "WaitKeyContainer{" + "key=" + key + ", retries=" + retries() + '}';
    }
}
