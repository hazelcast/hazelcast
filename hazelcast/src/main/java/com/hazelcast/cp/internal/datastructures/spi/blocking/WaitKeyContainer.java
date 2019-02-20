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

    private W key;

    private List<W> retries;

    public WaitKeyContainer() {
    }

    WaitKeyContainer(W key) {
        this.key = key;
    }

    public W key() {
        return key;
    }

    public List<W> retries() {
        return retries != null ? retries : Collections.<W>emptyList();
    }

    public List<W> keyAndRetries() {
        if (retryCount() == 0) {
            return Collections.singletonList(key);
        }

        List<W> all = new ArrayList<W>(1 + retryCount());
        all.add(key);
        all.addAll(retries());

        return all;
    }

    public int retryCount() {
        return retries().size();
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
        if (retries == null) {
            retries = new ArrayList<W>(1);
        }

        retries.add(retry);
    }

    @Override
    public int getFactoryId() {
        return RaftDataServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftDataServiceDataSerializerHook.WAIT_KEY_CONTAINER;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(key);
        int retryCount = retries != null ? retries.size() : 0;
        out.writeInt(retryCount);
        for (int i = 0; i < retryCount; i++) {
            out.writeObject(retries.get(i));
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        key = in.readObject();
        int retryCount = in.readInt();
        if (retryCount > 0) {
            retries = new ArrayList<W>(retryCount);
            for (int i = 0; i < retryCount; i++) {
                W retry = in.readObject();
                retries.add(retry);
            }
        }
    }

    @Override
    public String toString() {
        return "WaitKeyContainer{" + "key=" + key + ", retries=" + retries + '}';
    }
}
