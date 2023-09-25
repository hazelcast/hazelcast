/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.schema.dataconnection;

import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.jet.sql.impl.parse.SqlCreateDataConnection;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.schema.SqlCatalogObject;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static com.hazelcast.dataconnection.impl.DataConnectionServiceImpl.DataConnectionSource;

/**
 * The value in catalog map for data connections.
 */
public class DataConnectionCatalogEntry implements SqlCatalogObject {
    private String name;
    private String type;
    private boolean shared;
    private Map<String, String> options;
    private DataConnectionSource source;

    public DataConnectionCatalogEntry() {
    }

    public DataConnectionCatalogEntry(String name, String type, boolean shared, Map<String, String> options) {
        this.name = name;
        this.type = type;
        this.shared = shared;
        this.options = options;
        this.source = DataConnectionSource.SQL;
    }

    public DataConnectionCatalogEntry(
            String name,
            String type,
            boolean shared,
            Map<String, String> options,
            DataConnectionSource source) {
        this.name = name;
        this.shared = shared;
        this.type = type;
        this.options = options;
        this.source = source;
    }

    @Override
    public String name() {
        return name;
    }

    @Nonnull
    public String type() {
        return type;
    }

    public boolean isShared() {
        return shared;
    }

    @Nonnull
    public Map<String, String> options() {
        return options;
    }

    @Nonnull
    public DataConnectionSource source() {
        return source;
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        type = in.readString();
        shared = in.readBoolean();
        options = in.readObject();
        source = in.readObject();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeString(type);
        out.writeBoolean(shared);
        out.writeObject(options);
        out.writeObject(source);
    }

    @Override
    public int getFactoryId() {
        return JetSqlSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.DATA_CONNECTION;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataConnectionCatalogEntry e = (DataConnectionCatalogEntry) o;
        return shared == e.shared && name.equals(e.name) && type.equals(e.type) && options.equals(e.options)
                && source == e.source;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, shared, options, source);
    }

    @Nonnull
    @Override
    public String unparse() {
        return SqlCreateDataConnection.unparse(this);
    }
}
