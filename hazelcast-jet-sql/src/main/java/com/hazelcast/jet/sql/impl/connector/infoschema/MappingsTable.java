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

package com.hazelcast.jet.sql.impl.connector.infoschema;

import com.hazelcast.jet.sql.impl.schema.MappingDefinition;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

/**
 * Table object for the {@code information_schema.mappings} table.
 */
public class MappingsTable extends InfoSchemaTable {

    private static final String NAME = "mappings";

    private static final List<TableField> FIELDS = asList(
            new TableField("mapping_catalog", QueryDataType.VARCHAR, false),
            new TableField("mapping_schema", QueryDataType.VARCHAR, false),
            new TableField("mapping_name", QueryDataType.VARCHAR, false),
            new TableField("mapping_type", QueryDataType.VARCHAR, false),
            new TableField("mapping_options", QueryDataType.VARCHAR, false)
    );

    private final String catalog;
    private final List<MappingDefinition> definitions;

    public MappingsTable(
            String catalog,
            String schemaName,
            List<MappingDefinition> definitions
    ) {
        super(
                FIELDS,
                schemaName,
                NAME,
                new ConstantTableStatistics(definitions.size())
        );

        this.catalog = catalog;
        this.definitions = definitions;
    }

    @Override
    protected List<Object[]> rows() {
        List<Object[]> rows = new ArrayList<>(definitions.size());
        for (MappingDefinition definition : definitions) {
            Object[] row = new Object[]{
                    catalog,
                    definition.schema(),
                    definition.name(),
                    definition.type(),
                    definition.options()
            };
            rows.add(row);
        }
        return rows;
    }
}
