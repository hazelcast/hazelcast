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

import com.hazelcast.sql.impl.type.QueryDataType;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hazelcast table which provides information about its fields.
 */
public final class HazelcastTable extends AbstractTable {

    private static final Map<QueryDataType, SqlTypeName> QUERY_TO_SQL_TYPE = new HashMap<>();

    static {
        QUERY_TO_SQL_TYPE.put(QueryDataType.VARCHAR, SqlTypeName.VARCHAR);
        QUERY_TO_SQL_TYPE.put(QueryDataType.VARCHAR_CHARACTER, SqlTypeName.CHAR);

        QUERY_TO_SQL_TYPE.put(QueryDataType.BIT, SqlTypeName.BOOLEAN);
        QUERY_TO_SQL_TYPE.put(QueryDataType.TINYINT, SqlTypeName.TINYINT);
        QUERY_TO_SQL_TYPE.put(QueryDataType.SMALLINT, SqlTypeName.SMALLINT);
        QUERY_TO_SQL_TYPE.put(QueryDataType.INT, SqlTypeName.INTEGER);
        QUERY_TO_SQL_TYPE.put(QueryDataType.BIGINT, SqlTypeName.BIGINT);

        QUERY_TO_SQL_TYPE.put(QueryDataType.DECIMAL, SqlTypeName.DECIMAL);
        QUERY_TO_SQL_TYPE.put(QueryDataType.DECIMAL_BIG_INTEGER, SqlTypeName.DECIMAL);

        QUERY_TO_SQL_TYPE.put(QueryDataType.REAL, SqlTypeName.REAL);
        QUERY_TO_SQL_TYPE.put(QueryDataType.DOUBLE, SqlTypeName.DOUBLE);

        QUERY_TO_SQL_TYPE.put(QueryDataType.TIME, SqlTypeName.TIME);
        QUERY_TO_SQL_TYPE.put(QueryDataType.DATE, SqlTypeName.DATE);
        QUERY_TO_SQL_TYPE.put(QueryDataType.TIMESTAMP, SqlTypeName.TIMESTAMP);
        QUERY_TO_SQL_TYPE.put(QueryDataType.TIMESTAMP_WITH_TZ_DATE, SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        QUERY_TO_SQL_TYPE.put(QueryDataType.TIMESTAMP_WITH_TZ_CALENDAR, SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        QUERY_TO_SQL_TYPE.put(QueryDataType.TIMESTAMP_WITH_TZ_INSTANT, SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        QUERY_TO_SQL_TYPE.put(QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        QUERY_TO_SQL_TYPE.put(QueryDataType.TIMESTAMP_WITH_TZ_ZONED_DATE_TIME, SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        QUERY_TO_SQL_TYPE.put(QueryDataType.INTERVAL_YEAR_MONTH, SqlTypeName.INTERVAL_YEAR_MONTH);
        QUERY_TO_SQL_TYPE.put(QueryDataType.INTERVAL_DAY_SECOND, SqlTypeName.INTERVAL_DAY_SECOND);

        QUERY_TO_SQL_TYPE.put(QueryDataType.OBJECT, SqlTypeName.ANY);
    }

    private final String schemaName;
    private final String name;
    private final boolean partitioned;
    private final String distributionField;
    private final List<HazelcastTableIndex> indexes;
    private final Statistic statistic;
    private final Map<String, QueryDataType> fieldTypes;
    private final Map<String, String> fieldPaths;

    private RelDataType rowType;

    public HazelcastTable(
        String schemaName,
        String name,
        boolean partitioned,
        String distributionField,
        List<HazelcastTableIndex> indexes,
        Map<String, QueryDataType> fieldTypes,
        Map<String, String> fieldPaths,
        Statistic statistic
    ) {
        this.schemaName = schemaName;
        this.name = name;
        this.partitioned = partitioned;
        this.distributionField = distributionField;
        this.fieldTypes = fieldTypes != null ? fieldTypes : Collections.emptyMap();
        this.fieldPaths = fieldPaths != null ? fieldPaths : Collections.emptyMap();
        this.indexes = indexes != null ? indexes : Collections.emptyList();
        this.statistic = statistic;
    }

    public QueryDataType getFieldType(String fieldName) {
        QueryDataType fieldType = fieldTypes.get(fieldName);

        if (fieldType == null) {
            fieldType = QueryDataType.LATE;
        }

        return fieldType;
    }

    public String getFieldPath(String fieldName) {
        String path = fieldPaths.get(fieldName);

        return path != null ? path : fieldName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getName() {
        return name;
    }

    public boolean isPartitioned() {
        return partitioned;
    }

    public boolean isReplicated() {
        return !isPartitioned();
    }

    public String getDistributionField() {
        return distributionField;
    }

    public List<HazelcastTableIndex> getIndexes() {
        return indexes;
    }

    public List<RelDataTypeField> getFieldList() {
        return rowType.getFieldList();
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (rowType != null) {
            return rowType;
        }

        List<RelDataTypeField> fields = new ArrayList<>(fieldTypes.size());
        for (Map.Entry<String, QueryDataType> entry : fieldTypes.entrySet()) {
            SqlTypeName sqlTypeName = QUERY_TO_SQL_TYPE.get(entry.getValue());
            assert sqlTypeName != null;
            RelDataType relDataType = typeFactory.createSqlType(sqlTypeName);
            RelDataType nullableRelDataType = typeFactory.createTypeWithNullability(relDataType, true);
            RelDataTypeField field = new RelDataTypeFieldImpl(entry.getKey(), fields.size(), nullableRelDataType);
            fields.add(field);
        }
        rowType = new RelRecordType(StructKind.PEEK_FIELDS, fields, false);

        return rowType;
    }

    @Override
    public final Statistic getStatistic() {
        return statistic;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{name=" + name + ", partitioned=" + partitioned
            + ", distributionField=" + distributionField + ", indexes=" + indexes + '}';
    }

}
