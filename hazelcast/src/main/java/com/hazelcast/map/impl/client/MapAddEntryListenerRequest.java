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

package com.hazelcast.map.impl.client;

import com.hazelcast.map.impl.MapPortableHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.query.Predicate;
import java.io.IOException;

public class MapAddEntryListenerRequest extends AbstractMapAddEntryListenerRequest {

    private Predicate predicate;

    public MapAddEntryListenerRequest() {
        super();
    }

    public MapAddEntryListenerRequest(String name, boolean includeValue) {
        super(name, includeValue);
    }

    public MapAddEntryListenerRequest(String name, Data key, boolean includeValue) {
        super(name, key, includeValue);
    }

    public MapAddEntryListenerRequest(String name, Data key, boolean includeValue, Predicate predicate) {
        super(name, key, includeValue);
        this.predicate = predicate;
    }

    @Override
    public int getClassId() {
        return MapPortableHook.ADD_ENTRY_LISTENER;
    }

    @Override
    protected Predicate getPredicate() {
        return predicate;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("name", name);
        writer.writeBoolean("i", includeValue);

        final boolean hasKey = key != null;
        writer.writeBoolean("key", hasKey);
        if (predicate == null) {
            writer.writeBoolean("pre", false);
            if (hasKey) {
                final ObjectDataOutput out = writer.getRawDataOutput();
                out.writeData(key);
            }
        } else {
            writer.writeBoolean("pre", true);
            final ObjectDataOutput out = writer.getRawDataOutput();
            out.writeObject(predicate);
            if (hasKey) {
                out.writeData(key);
            }
        }
        super.write(writer);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("name");
        includeValue = reader.readBoolean("i");

        boolean hasKey = reader.readBoolean("key");
        if (reader.readBoolean("pre")) {
            final ObjectDataInput in = reader.getRawDataInput();
            predicate = in.readObject();
            if (hasKey) {
                key = in.readData();
            }
        } else if (hasKey) {
            final ObjectDataInput in = reader.getRawDataInput();
            key = in.readData();
        }
        super.read(reader);
    }

    @Override
    public String getMethodName() {
        return "addEntryListener";
    }

    @Override
    public Object[] getParameters() {
        if (key == null && predicate == null) {
            return new Object[]{null, includeValue};
        } else if (predicate == null) {
            return new Object[]{null, key, includeValue};
        } else if (key == null) {
            return new Object[]{null, predicate, includeValue};
        }
        return new Object[]{null, predicate, key, includeValue};
    }
}
