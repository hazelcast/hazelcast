/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.connector.infoschema;

import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.Mapping;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.view.View;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;

/**
 * Table object for the {@code information_schema.tables} table.
 */
public class TablesTable extends InfoSchemaTable {

    private static final String NAME = "tables";

    private static final List<TableField> FIELDS = asList(
            new TableField("table_catalog", QueryDataType.VARCHAR, false),
            new TableField("table_schema", QueryDataType.VARCHAR, false),
            new TableField("table_name", QueryDataType.VARCHAR, false),
            new TableField("table_type", QueryDataType.VARCHAR, false),
            // FYI: standard tables, which is unused at the moment : check_option, is_updatable and insertable_into
            new TableField("self_referencing_column_name", QueryDataType.VARCHAR, false),
            new TableField("reference_generation", QueryDataType.VARCHAR, false),
            new TableField("user_defined_type_catalog", QueryDataType.VARCHAR, false),
            new TableField("user_defined_type_schema", QueryDataType.VARCHAR, false),
            new TableField("user_defined_type_name", QueryDataType.VARCHAR, false),
            new TableField("is_insertable_into", QueryDataType.VARCHAR, false),
            new TableField("is_typed", QueryDataType.VARCHAR, false),
            new TableField("commit_action", QueryDataType.VARCHAR, false)
    );

    private final String schema;
    private final Collection<Mapping> mappings;
    private final Collection<View> views;

    public TablesTable(
            String catalog,
            String schemaName,
            String schema,
            Collection<Mapping> mappings,
            Collection<View> views
    ) {
        super(FIELDS, catalog, schemaName, NAME, new ConstantTableStatistics(views.size()));

        this.schema = schema;
        this.mappings = mappings;
        this.views = views;
    }

    @Override
    protected List<Object[]> rows() {
        List<Object[]> rows = new ArrayList<>(views.size());
        for (Mapping m : mappings) {
            rows.add(new Object[]{
                    catalog(),
                    schema,
                    m.name(),
                    // The SQL standard allows only 4 values: BASE TABLE, VIEW, GLOBAL TEMPORARY and LOCAL TEMPORARY.
                    // For VIEW it requires an entry in information_schema.views. So that's not an option.
                    // It's also not temporary. Therefore, we're left with BASE TABLE.
                    "BASE TABLE",
                    null,
                    null,
                    null,
                    null,
                    null,
                    "YES", // is_insertable_into
                    "NO",  // is_typed
                    null // not a temporary table
            });
        }
        for (View v : views) {
            rows.add(new Object[]{
                    catalog(),
                    schema,
                    v.name(),
                    "VIEW",
                    null,
                    null,
                    null,
                    null,
                    null,
                    "NO", // is_insertable_into
                    "NO",  // is_typed
                    null // not a temporary table
            });
        }
        return rows;
    }
}
