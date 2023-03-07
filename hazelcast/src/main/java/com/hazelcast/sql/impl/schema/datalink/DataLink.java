/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.schema.datalink;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.schema.SqlCatalogObject;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * SQL schema POJO class.
 */
public class DataLink implements IdentifiedDataSerializable, SqlCatalogObject {
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

    @Override
    public String unparse() {
        StringBuilder buffer = new StringBuilder();

        buffer.append("CREATE OR REPLACE DATA LINK ");
        buffer.append("\"hazelcast\".\"public\".").append("\"").append(name).append("\" ");

        buffer.append("TYPE").append(" \"");
        buffer.append(type).append("\" ");

        buffer.append("OPTIONS").append(" (");
        if (options.size() > 0) {
            int optionsSize = options.size() - 1;
            for (Map.Entry<String, String> option : options.entrySet()) {
                buffer.append("'")
                        .append(option.getKey())
                        .append("'")
                        .append(" = ")
                        .append("'")
                        .append(option.getValue())
                        .append("'");
                if (optionsSize-- > 0) {
                    buffer.append(",\n");
                }
            }
        }
        buffer.append(")\n");

        return buffer.toString();
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
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.DATA_LINK;
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
