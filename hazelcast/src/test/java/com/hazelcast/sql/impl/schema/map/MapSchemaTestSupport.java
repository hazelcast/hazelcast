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

package com.hazelcast.sql.impl.schema.map;

import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.type.QueryDataType;

import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.KEY_PATH;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE_PATH;

public class MapSchemaTestSupport extends SqlTestSupport {
    protected MapSchemaTestSupport() {
        // No-op.
    }

    public static MapTableField field(String name, QueryDataType type, boolean key) {
        return field0(name, type, key, false);
    }

    public static MapTableField hiddenField(String name, QueryDataType type, boolean key) {
        return field0(name, type, key, true);
    }

    private static MapTableField field0(String name, QueryDataType type, boolean key, boolean hidden) {
        QueryPath path = name.equals(KEY) && key ? KEY_PATH : name.equals(VALUE) && !key ? VALUE_PATH : new QueryPath(name, key);

        return new MapTableField(name, type, hidden, path);
    }
}
