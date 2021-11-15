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
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.view.View;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;

/**
 * Table object for the {@code information_schema.views} table.
 */
public class ViewsTable extends InfoSchemaTable {

    private static final String NAME = "views";

    private static final List<TableField> FIELDS = asList(
            new TableField("table_catalog", QueryDataType.VARCHAR, false),
            new TableField("table_schema", QueryDataType.VARCHAR, false),
            new TableField("table_name", QueryDataType.VARCHAR, false),
            new TableField("view_definition", QueryDataType.VARCHAR, false),
            // FYI: standard tables, which is unused at the moment : check_option, is_updatable and insertable_into
            new TableField("check_option", QueryDataType.VARCHAR, false),
            new TableField("is_updatable", QueryDataType.VARCHAR, false),
            new TableField("insertable_into", QueryDataType.VARCHAR, false)
    );

    private final String mappingsSchema;
    private final Collection<View> views;

    public ViewsTable(
            String catalog,
            String schemaName,
            String mappingsSchema,
            Collection<View> views
    ) {
        super(
                FIELDS,
                catalog,
                schemaName,
                NAME,
                new ConstantTableStatistics(views.size())
        );

        this.mappingsSchema = mappingsSchema;
        this.views = views;
    }

    @Override
    protected List<Object[]> rows() {
        List<Object[]> rows = new ArrayList<>(views.size());
        for (View v : views) {
            Object[] row = new Object[]{
                    catalog(),
                    mappingsSchema,
                    v.name(),
                    v.query(),
                    "NONE",     // check_option, NONE by default
                    "NO",       // is_updatable,    NO by default
                    "NO"        // insertable_into, NO by default
            };
            rows.add(row);
        }
        return rows;
    }
}
