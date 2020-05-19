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

import java.util.List;

/**
 * Generic table metadata.
 */
public abstract class Table {

    private final String schemaName;
    private final String name;
    private final List<TableField> fields;
    private final TableStatistics statistics;

    protected Table(String schemaName, String name, List<TableField> fields, TableStatistics statistics) {
        this.schemaName = schemaName;
        this.name = name;
        this.fields = fields;
        this.statistics = statistics;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getName() {
        return name;
    }

    public int getFieldCount() {
        return fields.size();
    }

    @SuppressWarnings("unchecked")
    public <T extends TableField> T getField(int index) {
        return (T) fields.get(index);
    }

    public TableStatistics getStatistics() {
        return statistics;
    }
}
