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

package com.hazelcast.sql.impl.schema.map;

import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;

/**
 * Field of IMap or ReplicatedMap.
 */
public class MapTableField extends TableField {
    /** Path to the field. */
    private final QueryPath path;

    public MapTableField(String name, QueryDataType type, boolean hidden, QueryPath path) {
        super(name, type, hidden);

        this.path = path;
    }

    public QueryPath getPath() {
        return path;
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

        MapTableField field = (MapTableField) o;

        return path.equals(field.path);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();

        result = 31 * result + path.hashCode();

        return result;
    }

    @Override
    public String toString() {
        return "MapTableField{name=" + name + ", type=" + type + ", path=" + path + ", hidden=" + hidden + '}';
    }
}
