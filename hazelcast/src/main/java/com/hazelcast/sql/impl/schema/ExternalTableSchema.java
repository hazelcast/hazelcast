/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.schema;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * User defined table schema definition.
 */
public class ExternalTableSchema implements DataSerializable {

    private String name;
    private String type;
    private List<Field> fields;
    private Map<String, String> options;

    @SuppressWarnings("unused")
    private ExternalTableSchema() {
    }

    public ExternalTableSchema(String name,
                               String type,
                               List<Field> fields,
                               Map<String, String> options) {
        this.name = name;
        this.type = type;
        this.fields = fields;
        this.options = options;
    }

    public String name() {
        return name;
    }

    public String type() {
        return type;
    }

    public List<Field> fields() {
        return Collections.unmodifiableList(fields);
    }

    public Map<String, String> options() {
        return Collections.unmodifiableMap(options);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(type);
        out.writeObject(fields);
        out.writeObject(options);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        type = in.readUTF();
        fields = in.readObject();
        options = in.readObject();
    }

    public static class Field implements DataSerializable {

        private static final String NAME = "name";
        private static final String TYPE = "type";

        private Map<String, Object> properties;

        @SuppressWarnings("unused")
        private Field() {
        }

        public Field(String name, QueryDataType type) {
            this.properties = new HashMap<>();
            this.properties.put(NAME, name);
            this.properties.put(TYPE, type);
        }

        public String name() {
            return Objects.requireNonNull((String) properties.get(NAME), "missing name property");
        }

        public QueryDataType type() {
            return Objects.requireNonNull((QueryDataType) properties.get(TYPE), "missing type property");
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(properties);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            properties = in.readObject();
        }
    }
}
