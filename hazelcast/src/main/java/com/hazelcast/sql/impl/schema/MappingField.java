/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * A field in a {@link Mapping}.
 */
public class MappingField implements IdentifiedDataSerializable {

    private static final String NAME = "name";
    private static final String TYPE = "type";
    private static final String EXTERNAL_NAME = "externalName";

    // This generic structure is used to have binary compatibility if more fields are added in
    // the future, like nullability, uniqueness etc. Instances are stored as a part of the
    // persisted schema.
    private Map<String, Object> properties;

    public MappingField() {
    }

    public MappingField(String name, QueryDataType type) {
        this(name, type, null);
    }

    public MappingField(String name, QueryDataType type, String externalName) {
        this.properties = new HashMap<>();
        this.properties.put(NAME, requireNonNull(name));
        this.properties.put(TYPE, requireNonNull(type));
        if (externalName != null) {
            this.properties.put(EXTERNAL_NAME, externalName);
        }
    }

    /**
     * Column name. This is the name that is seen in SQL queries.
     */
    public String name() {
        return requireNonNull((String) properties.get(NAME), "missing name property");
    }

    public QueryDataType type() {
        return requireNonNull((QueryDataType) properties.get(TYPE), "missing type property");
    }

    /**
     * The external name of a field. For example, in case of IMap or Kafka,
     * it always starts with `__key` or `this`.
     */
    public String externalName() {
        return (String) properties.get(EXTERNAL_NAME);
    }

    public void setExternalName(String extName) {
        properties.put(EXTERNAL_NAME, extName);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.MAPPING_FIELD;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(properties);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        properties = in.readObject();
    }

    @Override
    public String toString() {
        return "MappingField{"
                + "properties=" + properties
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MappingField that = (MappingField) o;
        return Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(properties);
    }
}
