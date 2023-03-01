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

import java.util.Objects;

class MongoTableField extends TableField {
    final String externalName;
    private final BsonType bsonType;

    MongoTableField(String name, QueryDataType type, String externalName, boolean hidden, BsonType bsonType) {
        super(name, type, hidden);
        this.externalName = externalName;
        this.bsonType = bsonType;
    }

    public String getExternalName() {
        return externalName;
    }

    public BsonType getBsonType() {
        return bsonType;
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
        return Objects.equals(externalName, that.externalName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), externalName);
    }
}
