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

package com.hazelcast.sql;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.sql.impl.SqlDataSerializerHook;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * SQL column metadata.
 */
public final class SqlColumnMetadata implements IdentifiedDataSerializable {

    private String name;
    private SqlColumnType type;
    private boolean nullable;

    public SqlColumnMetadata() {
    }

    @PrivateApi
    @SuppressWarnings("ConstantConditions")
    public SqlColumnMetadata(@Nonnull String name, @Nonnull SqlColumnType type, boolean nullable) {
        assert name != null;
        assert type != null;

        this.name = name;
        this.type = type;
        this.nullable = nullable;
    }

    /**
     * Get column name.
     *
     * @return column name
     */
    @Nonnull
    public String getName() {
        return name;
    }

    /**
     * Gets column type.
     *
     * @return column type
     */
    @Nonnull
    public SqlColumnType getType() {
        return type;
    }

    /**
     * Gets column nullability.
     *
     * @return column type
     */
    public boolean isNullable() {
        return nullable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SqlColumnMetadata that = (SqlColumnMetadata) o;

        if (!name.equals(that.name)) {
            return false;
        }

        return nullable == that.nullable && type == that.type;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();

        result = 31 * result + type.hashCode();
        result = 31 * result + Boolean.hashCode(nullable);

        return result;
    }

    @Override
    public String toString() {
        return name + ' ' + type;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeObject(type);
        out.writeBoolean(nullable);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        type = in.readObject();
        nullable = in.readBoolean();
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;

    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.SQL_COLUMN_METADATA;
    }
}
