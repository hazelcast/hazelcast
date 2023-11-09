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

import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.Objects;

/**
 * Base class for all table fields. Different backends may have additional
 * metadata associated with the field.
 */
public class TableField {

    protected final String name;
    protected final QueryDataType type;
    protected final boolean hidden;

    public TableField(String name, QueryDataType type, boolean hidden) {
        this.name = name;
        this.type = type;
        this.hidden = hidden;
    }

    public String getName() {
        return name;
    }

    public QueryDataType getType() {
        return type;
    }

    public boolean isHidden() {
        return hidden;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableField that = (TableField) o;
        return name.equals(that.name) && type.equals(that.type) && hidden == that.hidden;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, hidden);
    }
}
