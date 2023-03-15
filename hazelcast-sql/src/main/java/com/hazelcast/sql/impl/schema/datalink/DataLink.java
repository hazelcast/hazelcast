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

package com.hazelcast.sql.impl.schema.datalink;

import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.jet.sql.impl.parse.SqlCreateDataLink;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.schema.SqlCatalogObject;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * SQL schema POJO class.
 */
public class DataLink implements SqlCatalogObject {
    private String name;
    private String type;
    private Map<String, String> options;

    public DataLink() {
    }

    public DataLink(String name, String type, Map<String, String> options) {
        this.name = name;
        this.type = type;
        this.options = options;
    }

    public String name() {
        return name;
    }

    public String type() {
        return type;
    }

    public Map<String, String> options() {
        return options;
    }

    @Nonnull
    @Override
    public String unparse() {
        return SqlCreateDataLink.unparse(this);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        type = in.readString();
        options = in.readObject();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeString(type);
        out.writeObject(options);
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.DATA_LINK;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataLink dataLink = (DataLink) o;
        return name.equals(dataLink.name) && type.equals(dataLink.type) && options.equals(dataLink.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, options);
    }
}
