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

import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.sql.impl.schema.Mapping;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static java.util.Arrays.asList;

/**
 * Table object for the {@code information_schema.mappings} table.
 */
public class MappingsTable extends InfoSchemaTable {

    private static final String NAME = "mappings";

    private static final List<TableField> FIELDS = asList(
            new TableField("table_catalog", QueryDataType.VARCHAR, false),
            new TableField("table_schema", QueryDataType.VARCHAR, false),
            new TableField("table_name", QueryDataType.VARCHAR, false),
            new TableField("mapping_external_name", QueryDataType.VARCHAR, false),
            new TableField("mapping_type", QueryDataType.VARCHAR, false),
            new TableField("mapping_options", QueryDataType.VARCHAR, false)
    );

    private final String mappingsSchema;
    private final Collection<Mapping> mappings;

    public MappingsTable(
            String catalog,
            String schemaName,
            String mappingsSchema,
            Collection<Mapping> mappings
    ) {
        super(
                FIELDS,
                catalog,
                schemaName,
                NAME,
                new ConstantTableStatistics(mappings.size())
        );

        this.mappingsSchema = mappingsSchema;
        this.mappings = mappings;
    }

    @Override
    protected List<Object[]> rows() {
        List<Object[]> rows = new ArrayList<>(mappings.size());
        for (Mapping mapping : mappings) {
            Object[] row = new Object[]{
                    catalog(),
                    mappingsSchema,
                    mapping.name(),
                    mapping.externalName(),
                    mapping.type(),
                    uncheckCall(() -> JsonUtil.toJson(mapping.options()))
            };
            rows.add(row);
        }
        return rows;
    }
}
