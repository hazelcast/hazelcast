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

package com.hazelcast.sql.impl.schema;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
    private static final String EXTERNAL_TYPE = "externalType";
    private static final String PRIMARY_KEY = "primaryKey";

    // This generic structure is used to have binary compatibility if more fields are added in
    // the future, like nullability, uniqueness etc. Instances are stored as a part of the
    // persisted schema.
    private Map<String, Object> properties;

    public MappingField() {
    }

    public MappingField(String name, QueryDataType type) {
        this(name, type, null, null);
    }
    public MappingField(String name, QueryDataType type, String externalName) {
        this(name, type, externalName, null);
    }

    public MappingField(String name, QueryDataType type, String externalName, String externalType) {
        this.properties = new HashMap<>();
        this.properties.put(NAME, requireNonNull(name));
        this.properties.put(TYPE, requireNonNull(type));
        if (externalName != null) {
            this.properties.put(EXTERNAL_NAME, externalName);
        }
        if (externalType != null) {
            this.properties.put(EXTERNAL_TYPE, externalType);
        }
    }

    /**
     * Column name. This is the name that is seen in SQL queries.
     */
    @Nonnull
    public String name() {
        return requireNonNull((String) properties.get(NAME), "missing name property");
    }

    @Nonnull
    public QueryDataType type() {
        return requireNonNull((QueryDataType) properties.get(TYPE), "missing type property");
    }

    @Nonnull
    public MappingField setType(QueryDataType type) {
        properties.put(TYPE, type);
        return this;
    }

    /**
     * Returns what is specified after {@code EXTERNAL NAME} keyword in the corresponding
     * SQL mapping column. The interpretation of <em>external name</em> is up to the connector.
     * For example, in IMap and Kafka, the external name is fully-qualified: it starts with
     * {@code __key.} or {@code this.}.
     */
    public String externalName() {
        return (String) properties.get(EXTERNAL_NAME);
    }

    @Nonnull
    public MappingField setExternalName(String extName) {
        properties.put(EXTERNAL_NAME, extName);
        return this;
    }

    public boolean isPrimaryKey() {
        return (Boolean) properties.getOrDefault(PRIMARY_KEY, Boolean.FALSE);
    }

    @Nonnull
    public MappingField setPrimaryKey(boolean primaryKey) {
        properties.put(PRIMARY_KEY, primaryKey);
        return this;
    }

    /**
     * External type - for example for MongoDB it will be the BsonType,
     * for normal database it will be the SQL type, etc.
     *
     * @since 5.3
     */
    @Nullable
    public String externalType() {
        return (String) properties.get(EXTERNAL_TYPE);
    }

    @Nonnull
    public MappingField setExternalType(@Nonnull String externalType) {
        properties.put(EXTERNAL_TYPE, externalType);
        return this;
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
