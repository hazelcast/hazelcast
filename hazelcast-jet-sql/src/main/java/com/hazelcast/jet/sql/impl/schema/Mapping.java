/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An object stored in the internal storage for mappings created using DDL.
 */
public class Mapping implements DataSerializable {

    private String name;
    private String externalName;
    private String type;
    private List<MappingField> mappingFields;
    private Map<String, String> options;

    @SuppressWarnings("unused")
    private Mapping() {
    }

    public Mapping(
            String name,
            String externalName,
            String type,
            List<MappingField> fields,
            Map<String, String> options
    ) {
        this.name = name;
        this.externalName = externalName;
        this.type = type;
        this.mappingFields = fields;
        this.options = options;
    }

    public String name() {
        return name;
    }

    public String externalName() {
        return externalName;
    }

    public String type() {
        return type;
    }

    public List<MappingField> fields() {
        return Collections.unmodifiableList(mappingFields);
    }

    public Map<String, String> options() {
        return Collections.unmodifiableMap(options);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeString(externalName);
        out.writeString(type);
        out.writeObject(mappingFields);
        out.writeObject(options);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        externalName = in.readString();
        type = in.readString();
        mappingFields = in.readObject();
        options = in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Mapping mapping = (Mapping) o;
        return Objects.equals(name, mapping.name) &&
                Objects.equals(externalName, mapping.externalName) &&
                Objects.equals(type, mapping.type) &&
                Objects.equals(mappingFields, mapping.mappingFields) &&
                Objects.equals(options, mapping.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, externalName, type, mappingFields, options);
    }
}
