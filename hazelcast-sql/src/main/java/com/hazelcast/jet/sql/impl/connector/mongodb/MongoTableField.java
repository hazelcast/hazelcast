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
package com.hazelcast.jet.sql.impl.connector.mongodb;

import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.bson.BsonType;

import javax.annotation.Nonnull;
import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

class MongoTableField extends TableField {
    final String externalName;
    final BsonType externalType;
    final boolean primaryKey;

    MongoTableField(@Nonnull String name, @Nonnull QueryDataType type, @Nonnull String externalName, boolean hidden,
                    @Nonnull String externalType, boolean primaryKey) {
        super(name, type, hidden);
        checkNotNull(externalName, "external name cannot be null");
        checkNotNull(externalType, "external type cannot be null");
        this.externalName = externalName;
        this.externalType = BsonType.valueOf(externalType);
        this.primaryKey = primaryKey;
    }

    public String getExternalName() {
        return externalName;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    public BsonType getExternalType() {
        return externalType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MongoTableField)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        MongoTableField that = (MongoTableField) o;
        return primaryKey == that.primaryKey
                && Objects.equals(externalName, that.externalName)
                && Objects.equals(externalType, that.externalType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), externalName, primaryKey, externalType);
    }
}
