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

import com.hazelcast.sql.impl.type.QueryDataType;

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

        TableField field = (TableField) o;

        return name.equals(field.name) && type.equals(field.type) && hidden == field.hidden;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();

        result = 31 * result + type.hashCode();
        result = 31 * result + (hidden ? 1 : 0);

        return result;
    }
}
