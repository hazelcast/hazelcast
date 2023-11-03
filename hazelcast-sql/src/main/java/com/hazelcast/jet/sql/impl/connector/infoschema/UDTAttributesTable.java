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

import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.type.Type;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

public class UDTAttributesTable extends InfoSchemaTable {
    private static final String NAME = "attributes";

    private static final List<TableField> FIELDS = asList(
            new TableField("udt_catalog", QueryDataType.VARCHAR, false),
            new TableField("udt_schema", QueryDataType.VARCHAR, false),
            new TableField("udt_name", QueryDataType.VARCHAR, false),
            new TableField("attribute_name", QueryDataType.VARCHAR, false),
            new TableField("ordinal_position", QueryDataType.INT, false),
            new TableField("attribute_default", QueryDataType.VARCHAR, false),
            new TableField("is_nullable", QueryDataType.VARCHAR, false),
            new TableField("data_type", QueryDataType.VARCHAR, false),
            new TableField("character_maximum_length", QueryDataType.INT, false),
            new TableField("character_octet_length", QueryDataType.VARCHAR, false),
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
            new TableField("attribute_udt_catalog", QueryDataType.VARCHAR, false),
            new TableField("attribute_udt_schema", QueryDataType.VARCHAR, false),
            new TableField("attribute_udt_name", QueryDataType.VARCHAR, false),
            new TableField("scope_catalog", QueryDataType.VARCHAR, false),
            new TableField("scope_schema", QueryDataType.VARCHAR, false),
            new TableField("scope_name", QueryDataType.VARCHAR, false),
            new TableField("maximum_cardinality", QueryDataType.INT, false),
            new TableField("dtd_identifier", QueryDataType.VARCHAR, false),
            new TableField("is_derived_reference_attribute", QueryDataType.VARCHAR, false)
    );

    private final String schema;
    private final List<Type> types;

    public UDTAttributesTable(String catalog, String schemaName, String typesSchema, final List<Type> types) {
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
            List<Type.TypeField> fields = type.getFields();
            for (int i = 0, fieldsSize = fields.size(); i < fieldsSize; i++) {
                final Type.TypeField field = fields.get(i);
                rows.add(new Object[]{
                        catalog(),
                        schema,
                        type.name(),
                        field.getName(),
                        i + 1,
                        null, // attribute_default
                        "YES", // is_nullable
                        toSqlDataTypeString(field.getType()), // data type
                        field.getType().getTypeFamily() == QueryDataTypeFamily.VARCHAR
                                ? Integer.MAX_VALUE
                                : null, // character_maximum_length
                        field.getType().getTypeFamily() == QueryDataTypeFamily.VARCHAR
                                ? Integer.MAX_VALUE
                                : null, // character_octet_length
                        null, // character_set_catalog
                        null, // character_set_schema
                        null, // character_set_name
                        null, // collation_catalog
                        null, // collation_schema
                        null, // collation_name
                        field.getType().getTypeFamily().isNumeric()
                                ? getNumericTypePrecision(field.getType())
                                : null, // numeric_precision
                        field.getType().getTypeFamily().isNumeric()
                                ? 2
                                : null, // numeric_precision_radix
                        field.getType().getTypeFamily().isNumericInteger()
                                ? 0
                                : null, // numeric_scale
                        field.getType().getTypeFamily().isTemporal()
                                ? getTemporalTypePrecision(field.getType())
                                : null, // datetime_precision
                        null, // interval_type
                        null, // interval_precision
                        field.getType().isCustomType()
                                ? catalog()
                                : null, // attribute_udt_catalog
                        field.getType().isCustomType()
                                ? schema
                                : null, // attribute_udt_schema
                        field.getType().isCustomType()
                                ? field.getType().getObjectTypeName()
                                : null, // attribute_udt_name
                        null, // scope_catalog
                        null, // scope_schema
                        null, // scope_name
                        null, // maximum_cardinality
                        null, // dtd_identifier
                        null  // is_derived_reference_attribute
                });
            }
        }

        return rows;
    }

    private String toSqlDataTypeString(final QueryDataType dataType) {
        if (dataType.isCustomType()) {
            return "USER-DEFINED";
        }
        return HazelcastTypeUtils.toCalciteType(dataType).getName();
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    private Integer getTemporalTypePrecision(final QueryDataType dataType) {
        switch (dataType.getTypeFamily()) {
            case TIME:
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIME_ZONE:
                // number of digits after the dot when specifying seconds - 9 for nanoseconds
                return 9;
            case DATE:
            default:
                return 0;
        }

    }

    @SuppressWarnings("checkstyle:MagicNumber")
    private Integer getNumericTypePrecision(final QueryDataType dataType) {
        switch (dataType.getTypeFamily()) {
            case TINYINT:
                return 8;
            case SMALLINT:
                return 16;
            case INTEGER:
                return 32;
            case BIGINT:
                return 64;
            case REAL:
                return 24;
            case DOUBLE:
                return 53;
            default:
                return 0;
        }
    }
}
