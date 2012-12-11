/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.extensions.replicatedmap;

import com.hazelcast.core.Member;
import com.hazelcast.impl.MemberImpl;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.SerializationHelper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UpdateMessage<K, V> implements DataSerializable {
    K key;
    V value;
    Vector vector;
    Member origin;
    private boolean remove=false;

    public UpdateMessage() {
    }

    public UpdateMessage(K key, V v, Vector vector, Member origin, boolean remove) {
        this.key = key;
        this.value = v;
        this.vector = vector;
        this.origin = origin;
        this.remove = remove;
    }

    public UpdateMessage(K key, V v, Vector vector, Member origin) {
        this(key, v, vector, origin, false);
        
    }

    public void writeData(DataOutput dataOutput) throws IOException {
        SerializationHelper.writeObject(dataOutput, key);
        SerializationHelper.writeObject(dataOutput, value);
        vector.writeData(dataOutput);
        origin.writeData(dataOutput);
        dataOutput.writeBoolean(remove);

    }

    public void readData(DataInput dataInput) throws IOException {
        key = (K)SerializationHelper.readObject(dataInput);
        value = (V)SerializationHelper.readObject(dataInput);
        vector = new Vector();
        vector.readData(dataInput);
        origin = new MemberImpl();
        origin.readData(dataInput);
        remove = dataInput.readBoolean();
    }

    public boolean isRemove() {
        return remove;
    }

    @Override
    public String toString() {
        return "UpdateMessage{" +
                "key=" + key +
                ", value=" + value +
                ", vector=" + vector +
                ", origin=" + origin +
                ", remove=" + remove +
                '}';
    }
}
