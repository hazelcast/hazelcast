/*
 * Copyright 2023 Hazelcast Inc.
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
import com.hazelcast.sql.impl.schema.type.Type;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class UserDefinedTypesTable extends InfoSchemaTable {
    private static final String NAME = "user_defined_types";
    private static final List<TableField> FIELDS = Arrays.asList(
            new TableField("user_defined_type_catalog", QueryDataType.VARCHAR, false),
            new TableField("user_defined_type_schema", QueryDataType.VARCHAR, false),
            new TableField("user_defined_type_name", QueryDataType.VARCHAR, false),
            new TableField("user_defined_type_category", QueryDataType.VARCHAR, false),
            new TableField("is_instantiable", QueryDataType.VARCHAR, false),
            new TableField("is_final", QueryDataType.VARCHAR, false),
            new TableField("ordering_form", QueryDataType.VARCHAR, false),
            new TableField("ordering_category", QueryDataType.VARCHAR, false),
            new TableField("ordering_routine_catalog", QueryDataType.VARCHAR, false),
            new TableField("ordering_routine_schema", QueryDataType.VARCHAR, false),
            new TableField("ordering_routine_name", QueryDataType.VARCHAR, false),
            new TableField("reference_type", QueryDataType.VARCHAR, false),
            new TableField("data_type", QueryDataType.VARCHAR, false),
            new TableField("character_maximum_length", QueryDataType.INT, false),
            new TableField("character_octet_length", QueryDataType.INT, false),
            new TableField("character_set_catalog", QueryDataType.VARCHAR, false),
            new TableField("character_set_schema", QueryDataType.VARCHAR, false),
            new TableField("character_set_name", QueryDataType.VARCHAR, false),
            new TableField("collation_catalog", QueryDataType.VARCHAR, false),
            new TableField("collation_schema", QueryDataType.VARCHAR, false),
            new TableField("collation_name", QueryDataType.VARCHAR, false),
            new TableField("numeric_precision", QueryDataType.INT, false),
            new TableField("numeric_precision_radix", QueryDataType.INT, false),
            new TableField("numeric_scale", QueryDataType.INT, false),
            new TableField("datetime_precision", QueryDataType.INT, false),
            new TableField("interval_type", QueryDataType.VARCHAR, false),
            new TableField("interval_precision", QueryDataType.INT, false),
            new TableField("source_dtd_identifier", QueryDataType.VARCHAR, false),
            new TableField("ref_dtd_identifier", QueryDataType.VARCHAR, false)
    );

    private final String schema;
    private final List<Type> types;

    public UserDefinedTypesTable(String catalog, String schemaName, String typesSchema, final List<Type> types) {
        super(
                FIELDS,
                catalog,
                schemaName,
                NAME,
                new ConstantTableStatistics(0)
        );
        this.schema = typesSchema;
        this.types = types;
    }

    @Override
    protected List<Object[]> rows() {
        final List<Object[]> rows = new ArrayList<>();
        for (final Type type : types) {
            rows.add(new Object[] {
                catalog(),
                schema,
                type.name(),
                "STRUCTURED",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            });
        }

        return rows;
    }
}
