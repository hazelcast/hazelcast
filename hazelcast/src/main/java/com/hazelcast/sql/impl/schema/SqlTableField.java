/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
 * SQL table field descriptor.
 */
// TODO: Type nullability is not integrated! This will begatively affect compiler. Make sure to integrate it properly.
public class SqlTableField {
    private final String name;
    private final String path;
    private final QueryDataType type;

    public SqlTableField(String name, String path, QueryDataType type) {
        this.name = name;
        this.path = path;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public String getPath() {
        return path;
    }

    public QueryDataType getType() {
        return type;
    }

    @Override
    public String toString() {
        return "SqlTableField{name=" + name + ", path=" + path + ", type=" + type + '}';
    }
}
