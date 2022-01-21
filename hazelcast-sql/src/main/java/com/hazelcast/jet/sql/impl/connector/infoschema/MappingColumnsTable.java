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
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.view.View;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;

/**
 * Table object for the {@code information_schema.columns} table.
 */
public class MappingColumnsTable extends InfoSchemaTable {

    private static final String NAME = "columns";

    private static final List<TableField> FIELDS = asList(
            new TableField("table_catalog", QueryDataType.VARCHAR, false),
            new TableField("table_schema", QueryDataType.VARCHAR, false),
            new TableField("table_name", QueryDataType.VARCHAR, false),
            new TableField("column_name", QueryDataType.VARCHAR, false),
            new TableField("column_external_name", QueryDataType.VARCHAR, false),
            new TableField("ordinal_position", QueryDataType.INT, false),
            new TableField("is_nullable", QueryDataType.VARCHAR, false),
            new TableField("data_type", QueryDataType.VARCHAR, false)
    );

    private final String schema;
    private final Collection<Mapping> mappings;
    private final Collection<View> views;

    public MappingColumnsTable(
            String catalog,
            String schemaName,
            String schema,
            Collection<Mapping> mappings,
            Collection<View> views
    ) {
        super(
                FIELDS,
                catalog,
                schemaName,
                NAME,
                new ConstantTableStatistics((long) mappings.size() * FIELDS.size())
        );

        this.schema = schema;
        this.mappings = mappings;
        this.views = views;
    }

    @Override
    protected List<Object[]> rows() {
        List<Object[]> rows = new ArrayList<>(mappings.size());
        for (Mapping mapping : mappings) {
            List<MappingField> fields = mapping.fields();
            for (int i = 0; i < fields.size(); i++) {
                MappingField field = fields.get(i);
                Object[] row = new Object[]{
                        catalog(),
                        schema,
                        mapping.name(),
                        field.name(),
                        field.externalName(),
                        i + 1,
                        String.valueOf(true),
                        field.type().getTypeFamily().name()
                };
                rows.add(row);
            }
        }
        for (View view : views) {
            for (int i = 0; i < view.viewColumnNames().size(); i++) {
                Object[] row = new Object[]{
                        catalog(),
                        schema,
                        view.name(),
                        view.viewColumnNames().get(i),
                        null,
                        i + 1,
                        String.valueOf(true),
                        view.viewColumnTypes().get(i).getTypeFamily().name()
                };
                rows.add(row);
            }
        }
        return rows;
    }
}
