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

import com.hazelcast.sql.impl.plan.cache.PlanObjectKey;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Generic table metadata.
 */
public abstract class Table {

    private final String schemaName;
    private final String sqlName;
    private final List<TableField> fields;
    private final TableStatistics statistics;

    private Set<String> conflictingSchemas;

    protected Table(
        String schemaName,
        String sqlName,
        List<TableField> fields,
        TableStatistics statistics
    ) {
        this.schemaName = schemaName;
        this.sqlName = sqlName;
        this.fields = Collections.unmodifiableList(fields);
        this.statistics = statistics;
    }

    public String getSchemaName() {
        return schemaName;
    }

    /**
     * Returns the name of the table in SQL.
     */
    public String getSqlName() {
        return sqlName;
    }

    public List<TableField> getFields() {
        return fields;
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

    public abstract PlanObjectKey getObjectKey();

    public Set<String> getConflictingSchemas() {
        return conflictingSchemas != null ? conflictingSchemas : Collections.emptySet();
    }

    public void setConflictingSchemas(Set<String> conflictingSchemas) {
        this.conflictingSchemas = conflictingSchemas;
    }
}
