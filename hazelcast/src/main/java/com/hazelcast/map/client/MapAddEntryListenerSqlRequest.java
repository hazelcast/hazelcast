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

package com.hazelcast.map.client;

import com.hazelcast.map.MapPortableHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.predicate.SqlPredicate;

import java.io.IOException;

public class MapAddEntryListenerSqlRequest extends AbstractMapAddEntryListenerRequest {

    private String predicate;
    private transient Predicate cachedPredicate;

    public MapAddEntryListenerSqlRequest() {
        super();
    }

    public MapAddEntryListenerSqlRequest(String name, boolean includeValue) {
        super(name, includeValue);
    }

    public MapAddEntryListenerSqlRequest(String name, Data key, boolean includeValue) {
        super(name, key, includeValue);
    }

    public MapAddEntryListenerSqlRequest(String name, Data key, boolean includeValue, String predicate) {
        super(name, key, includeValue);
        this.predicate = predicate;
    }

    protected Predicate getPredicate() {
        if (cachedPredicate == null && predicate != null) {
            cachedPredicate = new SqlPredicate(predicate);
        }
        return cachedPredicate;
    }

    public int getClassId() {
        return MapPortableHook.ADD_ENTRY_LISTENER_SQL;
    }

    public void write(PortableWriter writer) throws IOException {
        final boolean hasKey = key != null;
        writer.writeBoolean("key", hasKey);
        if (predicate == null) {
            writer.writeBoolean("pre", false);
            if (hasKey) {
                final ObjectDataOutput out = writer.getRawDataOutput();
                key.writeData(out);
            }
        } else {
            writer.writeBoolean("pre", true);
            writer.writeUTF("p", predicate);
            final ObjectDataOutput out = writer.getRawDataOutput();
            if (hasKey) {
                key.writeData(out);
            }
        }

    }

    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("name");
        includeValue = reader.readBoolean("i");
        boolean hasKey = reader.readBoolean("key");
        if (reader.readBoolean("pre")) {
            predicate = reader.readUTF("p");
            final ObjectDataInput in = reader.getRawDataInput();
            if (hasKey) {
                key = new Data();
                key.readData(in);
            }
        } else if (hasKey) {
            final ObjectDataInput in = reader.getRawDataInput();
            key = new Data();
            key.readData(in);
        }
    }

}
