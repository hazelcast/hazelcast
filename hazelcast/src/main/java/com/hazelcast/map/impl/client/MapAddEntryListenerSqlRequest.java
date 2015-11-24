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

package com.hazelcast.map.impl.client;

import com.hazelcast.map.impl.MapPortableHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;

import java.io.IOException;

public class MapAddEntryListenerSqlRequest extends AbstractMapAddEntryListenerRequest {

    private String predicate;
    private transient Predicate cachedPredicate;

    public MapAddEntryListenerSqlRequest() {
    }

    public MapAddEntryListenerSqlRequest(String name, boolean includeValue, int listenerFlags) {
        super(name, includeValue, listenerFlags);
    }

    public MapAddEntryListenerSqlRequest(String name, Data key, boolean includeValue, int listenerFlags) {
        super(name, key, includeValue, listenerFlags);
    }

    public MapAddEntryListenerSqlRequest(String name, Data key, boolean includeValue, String predicate, int listenerFlags) {
        super(name, key, includeValue, listenerFlags);
        this.predicate = predicate;
    }

    @Override
    protected Predicate getPredicate() {
        if (cachedPredicate == null && predicate != null) {
            cachedPredicate = new SqlPredicate(predicate);
        }
        return cachedPredicate;
    }

    @Override
    public int getClassId() {
        return MapPortableHook.ADD_ENTRY_LISTENER_SQL;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        final boolean hasKey = key != null;
        writer.writeUTF("name", name);
        writer.writeBoolean("i", includeValue);
        writer.writeBoolean("key", hasKey);
        writer.writeInt("lf", listenerFlags);

        if (predicate == null) {
            writer.writeBoolean("pre", false);
            if (hasKey) {
                final ObjectDataOutput out = writer.getRawDataOutput();
                out.writeData(key);
            }
        } else {
            writer.writeBoolean("pre", true);
            writer.writeUTF("p", predicate);
            final ObjectDataOutput out = writer.getRawDataOutput();
            if (hasKey) {
                out.writeData(key);
            }
        }


    }

    @Override
    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        name = reader.readUTF("name");
        includeValue = reader.readBoolean("i");
        boolean hasKey = reader.readBoolean("key");
        listenerFlags = reader.readInt("lf");

        if (reader.readBoolean("pre")) {
            predicate = reader.readUTF("p");
            final ObjectDataInput in = reader.getRawDataInput();
            if (hasKey) {
                key = in.readData();
            }
        } else if (hasKey) {
            final ObjectDataInput in = reader.getRawDataInput();
            key = in.readData();
        }

    }

}
