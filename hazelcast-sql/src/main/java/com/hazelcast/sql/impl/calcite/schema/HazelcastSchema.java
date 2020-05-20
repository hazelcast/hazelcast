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

package com.hazelcast.sql.impl.calcite.schema;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Collections;
import java.util.Map;

/**
 * Implementation of a schema, exposing sub schemas and tables.
 */
public class HazelcastSchema extends AbstractSchema {

    private final Map<String, Schema> subSchemaMap;
    private final Map<String, Table> tableMap;

    public HazelcastSchema(Map<String, Table> tableMap) {
        this(null, tableMap);
    }

    public HazelcastSchema(Map<String, Schema> subSchemaMap, Map<String, Table> tableMap) {
        this.subSchemaMap = subSchemaMap != null ? subSchemaMap : Collections.emptyMap();
        this.tableMap = tableMap != null ? tableMap : Collections.emptyMap();
    }

    @Override
    protected Map<String, Schema> getSubSchemaMap() {
        return subSchemaMap;
    }

    @Override
    public Map<String, Table> getTableMap() {
        return tableMap;
    }

    @Override
    public Schema snapshot(SchemaVersion version) {
        return this;
    }
}
