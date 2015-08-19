/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.impl.messages;

import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.impl.operation.ReplicatedMapDataSerializerHook;
import java.io.IOException;

/**
 * This replication message is used for sending over a replication event to another node
 */
public class ReplicationMessage implements IdentifiedDataSerializable {

    private String name;
    private Data key;
    private Data value;
    private Member origin;
    private int updateHash;
    private long ttlMillis;

    public ReplicationMessage() {
    }

    public ReplicationMessage(String name, Data key, Data value, Member origin, int hash, long ttlMillis) {
        this.name = name;
        this.key = key;
        this.value = value;
        this.origin = origin;
        this.updateHash = hash;
        this.ttlMillis = ttlMillis;
    }

    public String getName() {
        return name;
    }

    public Data getDataKey() {
        return key;
    }

    public Data getDataValue() {
        return value;
    }

    public Member getOrigin() {
        return origin;
    }

    public long getTtlMillis() {
        return ttlMillis;
    }

    public int getUpdateHash() {
        return updateHash;
    }

    public boolean isRemove() {
        return value == null;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeData(key);
        out.writeData(value);
        origin.writeData(out);
        out.writeInt(updateHash);
        out.writeLong(ttlMillis);
    }

    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        key = in.readData();
        value = in.readData();
        origin = new MemberImpl();
        origin.readData(in);
        updateHash = in.readInt();
        ttlMillis = in.readLong();
    }

    @Override
    public int getFactoryId() {
        return ReplicatedMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ReplicatedMapDataSerializerHook.REPL_UPDATE_MESSAGE;
    }

    @Override
    public String toString() {
        return "ReplicationMessage{" + "key=" + key + ", value=" + value + ", origin=" + origin
                + ", updateHash=" + updateHash + ", ttlMillis=" + ttlMillis + '}';
    }

}
