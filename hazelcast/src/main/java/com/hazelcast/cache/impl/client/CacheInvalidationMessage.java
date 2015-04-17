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

package com.hazelcast.cache.impl.client;

import com.hazelcast.cache.impl.CachePortableHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public class CacheInvalidationMessage implements Portable {

    private String name;
    private Data key;
    private String sourceUuid;

    public CacheInvalidationMessage() {

    }

    public CacheInvalidationMessage(String name, Data key, String sourceUuid) {
        assert key == null || key.dataSize() > 0 : "Invalid invalidation key: " + key;
        this.name = name;
        this.key = key;
        this.sourceUuid = sourceUuid;
    }

    public String getName() {
        return name;
    }

    public Data getKey() {
        return key;
    }

    public String getSourceUuid() {
        return sourceUuid;
    }

    @Override
    public int getFactoryId() {
        return CachePortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CachePortableHook.INVALIDATION_MESSAGE;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeUTF("uuid", sourceUuid);
        ObjectDataOutput out = writer.getRawDataOutput();
        boolean hasKey = key != null;
        out.writeBoolean(hasKey);
        if (hasKey) {
            out.writeData(key);
        }
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        sourceUuid = reader.readUTF("uuid");
        ObjectDataInput in = reader.getRawDataInput();
        if (in.readBoolean()) {
            key = in.readData();
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CacheInvalidationMessage{");
        sb.append("name='").append(name).append('\'');
        sb.append(", key=").append(key);
        sb.append(", sourceUuid='").append(sourceUuid).append('\'');
        sb.append('}');
        return sb.toString();
    }

}
