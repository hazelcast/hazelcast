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

package com.hazelcast.collection;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * @mdogan 1/14/13
 */
public class CollectionProxyId implements DataSerializable {

    String name;

    String keyName;

    CollectionProxyType type;

    public CollectionProxyId() {
    }

    public CollectionProxyId(String name, String keyName, CollectionProxyType type) {
        this.name = name;
        this.keyName = keyName;
        this.type = type;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(type.getType());
        out.writeUTF(keyName);
    }

    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        type = CollectionProxyType.getByType(in.readInt());
        keyName = in.readUTF();
    }

    public String getName() {
        return name;
    }

    public String getKeyName() {
        return keyName;
    }

    public CollectionProxyType getType() {
        return type;
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CollectionProxyId)) return false;

        CollectionProxyId proxyId = (CollectionProxyId) o;

        if (keyName != null ? !keyName.equals(proxyId.keyName) : proxyId.keyName != null) return false;
        if (!name.equals(proxyId.name)) return false;
        if (type != proxyId.type) return false;

        return true;
    }

    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + (keyName != null ? keyName.hashCode() : 0);
        result = 31 * result + type.hashCode();
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("CollectionProxyId");
        sb.append("{name='").append(name).append('\'');
        sb.append(", keyName='").append(keyName).append('\'');
        sb.append(", type=").append(type);
        sb.append('}');
        return sb.toString();
    }
}
