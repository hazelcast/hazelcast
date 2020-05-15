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

import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base class for all tables in the Calcite integration:
 * <ul>
 *     <li>Maps field types defined in the {@code core} module to Calcite types</li>
 *     <li>Provides access to the underlying table and statistics</li>
 * </ul>
 */
public class HazelcastTable extends AbstractTable {

    private static final Map<QueryDataTypeFamily, SqlTypeName> QUERY_TO_SQL_TYPE = new HashMap<>();

    static {
        QUERY_TO_SQL_TYPE.put(QueryDataTypeFamily.VARCHAR, SqlTypeName.VARCHAR);

        QUERY_TO_SQL_TYPE.put(QueryDataTypeFamily.BOOLEAN, SqlTypeName.BOOLEAN);

        QUERY_TO_SQL_TYPE.put(QueryDataTypeFamily.TINYINT, SqlTypeName.TINYINT);
        QUERY_TO_SQL_TYPE.put(QueryDataTypeFamily.SMALLINT, SqlTypeName.SMALLINT);
        QUERY_TO_SQL_TYPE.put(QueryDataTypeFamily.INT, SqlTypeName.INTEGER);
        QUERY_TO_SQL_TYPE.put(QueryDataTypeFamily.BIGINT, SqlTypeName.BIGINT);

        QUERY_TO_SQL_TYPE.put(QueryDataTypeFamily.DECIMAL, SqlTypeName.DECIMAL);

        QUERY_TO_SQL_TYPE.put(QueryDataTypeFamily.REAL, SqlTypeName.REAL);
        QUERY_TO_SQL_TYPE.put(QueryDataTypeFamily.DOUBLE, SqlTypeName.DOUBLE);

        QUERY_TO_SQL_TYPE.put(QueryDataTypeFamily.TIME, SqlTypeName.TIME);
        QUERY_TO_SQL_TYPE.put(QueryDataTypeFamily.DATE, SqlTypeName.DATE);
        QUERY_TO_SQL_TYPE.put(QueryDataTypeFamily.TIMESTAMP, SqlTypeName.TIMESTAMP);
        QUERY_TO_SQL_TYPE.put(QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE, SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);

        QUERY_TO_SQL_TYPE.put(QueryDataTypeFamily.OBJECT, SqlTypeName.ANY);
    }

    private final Table target;
    private final Statistic statistic;

    private RelDataType rowType;

    public HazelcastTable(Table target, Statistic statistic) {
        this.target = target;
        this.statistic = statistic;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (rowType != null) {
            return rowType;
        }

        List<RelDataTypeField> convertedFields = new ArrayList<>(target.getFieldCount());

        for (int i = 0; i < target.getFieldCount(); i++) {
            TableField field = target.getField(i);

            String fieldName = field.getName();
            QueryDataType fieldType = field.getType();
            QueryDataTypeFamily fieldTypeFamily = fieldType.getTypeFamily();

            SqlTypeName sqlTypeName = QUERY_TO_SQL_TYPE.get(fieldTypeFamily);

            if (sqlTypeName == null) {
                throw new IllegalStateException("Unexpected type family: " + fieldTypeFamily);
            }

            RelDataType relDataType = typeFactory.createSqlType(sqlTypeName);
            RelDataType nullableRelDataType = typeFactory.createTypeWithNullability(relDataType, true);

            RelDataTypeField convertedField = new RelDataTypeFieldImpl(fieldName, convertedFields.size(), nullableRelDataType);
            convertedFields.add(convertedField);
        }

        rowType = new RelRecordType(StructKind.PEEK_FIELDS, convertedFields, false);

        return rowType;
    }

    @Override
    public Statistic getStatistic() {
        return statistic;
    }
}
