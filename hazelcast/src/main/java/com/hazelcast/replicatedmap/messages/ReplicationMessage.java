/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.messages;

import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.operation.ReplicatedMapDataSerializerHook;
import com.hazelcast.replicatedmap.record.Vector;

import java.io.IOException;

public class ReplicationMessage<K, V> implements IdentifiedDataSerializable {

    private String name;
    private K key;
    private V value;
    private Vector vector;
    private Member origin;
    private int updateHash;
    private long ttlMillis;

    public ReplicationMessage() {
    }

    public ReplicationMessage(String name, K key, V v, Vector vector, Member origin, int hash, long ttlMillis) {
        this.name = name;
        this.key = key;
        this.value = v;
        this.vector = vector;
        this.origin = origin;
        this.updateHash = hash;
        this.ttlMillis = ttlMillis;
    }

    public String getName() {
        return name;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public Vector getVector() {
        return vector;
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
        out.writeObject(key);
        out.writeObject(value);
        vector.writeData(out);
        origin.writeData(out);
        out.writeInt(updateHash);
        out.writeLong(ttlMillis);
    }

    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        key = (K) in.readObject();
        value = (V) in.readObject();
        vector = new Vector();
        vector.readData(in);
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
        return "ReplicationMessage{" +
               "key=" + key +
               ", value=" + value +
               ", vector=" + vector +
               ", origin=" + origin +
               ", updateHash=" + updateHash +
               ", ttlMillis=" + ttlMillis +
               '}';
    }

}
