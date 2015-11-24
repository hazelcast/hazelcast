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

package com.hazelcast.web;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Wrapper class which holds session attributes and jvmIds
 */

public class SessionState implements IdentifiedDataSerializable {

    private final Map<String, Data> attributes = new HashMap<String, Data>(1);
    private final Set<String> jvmIds = new HashSet<String>(1);

    @Override
    public int getFactoryId() {
        return WebDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return WebDataSerializerHook.SESSION_STATE;
    }

    public Map<String, Data> getAttributes() {
        return attributes;
    }

    public boolean addJvmId(String jvmId) {
        checkNotNull(jvmId, "JVM Id cannot be null.");
        return jvmIds.add(jvmId);
    }

    public boolean removeJvmId(String jvmId) {
        checkNotNull(jvmId, "JVM Id cannot be null.");
        return jvmIds.remove(jvmId);
    }

    public Set<String> getJvmIds() {
        return jvmIds;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(jvmIds.size());
        for (String jvmId : jvmIds) {
            out.writeUTF(jvmId);
        }
        out.writeInt(attributes.size());
        for (Map.Entry<String, Data> entry : attributes.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeData(entry.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int jvmCount = in.readInt();
        for (int i = 0; i < jvmCount; i++) {
            jvmIds.add(in.readUTF());
        }
        int attCount = in.readInt();
        for (int i = 0; i < attCount; i++) {
            attributes.put(in.readUTF(), in.readData());
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("SessionState {");
        sb.append("referenceCount=" + jvmIds.size());
        sb.append(", attributes=" + ((attributes == null) ? 0 : attributes.size()));
        if (attributes != null) {
            for (Map.Entry<String, Data> entry : attributes.entrySet()) {
                Data data = entry.getValue();
                int len = (data == null) ? 0 : data.dataSize();
                sb.append("\n\t");
                sb.append(entry.getKey() + "[" + len + "]");
            }
        }
        sb.append("\n}");
        return sb.toString();
    }
}
