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

package com.hazelcast.jet.sql.impl.connector.jdbc;

import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.Objects;

public class JdbcTableField extends TableField {

    private final String externalName;
    private final boolean primaryKey;

    public JdbcTableField(String name, QueryDataType type, String externalName, boolean primaryKey) {
        super(name, type, false);
        this.externalName = externalName;
        this.primaryKey = primaryKey;
    }

    public String externalName() {
        return externalName;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        JdbcTableField that = (JdbcTableField) o;
        return Objects.equals(externalName, that.externalName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), externalName);
    }
}
