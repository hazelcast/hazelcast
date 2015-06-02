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
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public class CacheSingleInvalidationMessage extends CacheInvalidationMessage {

    private Data key;
    private String sourceUuid;

    public CacheSingleInvalidationMessage() {

    }

    public CacheSingleInvalidationMessage(String name, Data key, String sourceUuid) {
        super(name);
        assert key == null || key.dataSize() > 0 : "Invalid invalidation key: " + key;
        assert sourceUuid != null : "Invalid source UUID: " + sourceUuid;
        this.key = key;
        this.sourceUuid = sourceUuid;
    }

    @Override
    public Data getKey() {
        return key;
    }

    public String getSourceUuid() {
        return sourceUuid;
    }

    @Override
    public int getClassId() {
        return CachePortableHook.INVALIDATION_MESSAGE;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        super.writePortable(writer);
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
        super.readPortable(reader);
        sourceUuid = reader.readUTF("uuid");
        ObjectDataInput in = reader.getRawDataInput();
        if (in.readBoolean()) {
            key = in.readData();
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CacheSingleInvalidationMessage{");
        sb.append("name='").append(name).append('\'');
        sb.append(", key=").append(key);
        sb.append(", sourceUuid='").append(sourceUuid).append('\'');
        sb.append('}');
        return sb.toString();
    }

}
