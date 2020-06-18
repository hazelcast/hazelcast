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

package com.hazelcast.sql.impl.calcite;

import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.calcite.sql.type.SqlTypeName.DAY_INTERVAL_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.YEAR_INTERVAL_TYPES;

/**
 * Provides utilities to map from Calcite's {@link SqlTypeName} to {@link
 * QueryDataType}.
 */
@SuppressWarnings("checkstyle:ExecutableStatementCount")
public final class SqlToQueryType {

    private static final Map<SqlTypeName, QueryDataType> CALCITE_TO_HZ = new HashMap<>();
    private static final Map<QueryDataTypeFamily, SqlTypeName> HZ_TO_CALCITE = new HashMap<>();

    static {
        HZ_TO_CALCITE.put(QueryDataTypeFamily.VARCHAR, SqlTypeName.VARCHAR);
        CALCITE_TO_HZ.put(SqlTypeName.VARCHAR, QueryDataType.VARCHAR);
        CALCITE_TO_HZ.put(SqlTypeName.CHAR, QueryDataType.VARCHAR);

        HZ_TO_CALCITE.put(QueryDataTypeFamily.BOOLEAN, SqlTypeName.BOOLEAN);
        CALCITE_TO_HZ.put(SqlTypeName.BOOLEAN, QueryDataType.BOOLEAN);

        HZ_TO_CALCITE.put(QueryDataTypeFamily.TINYINT, SqlTypeName.TINYINT);
        HZ_TO_CALCITE.put(QueryDataTypeFamily.SMALLINT, SqlTypeName.SMALLINT);
        HZ_TO_CALCITE.put(QueryDataTypeFamily.INT, SqlTypeName.INTEGER);
        HZ_TO_CALCITE.put(QueryDataTypeFamily.BIGINT, SqlTypeName.BIGINT);
        CALCITE_TO_HZ.put(SqlTypeName.TINYINT, QueryDataType.TINYINT);
        CALCITE_TO_HZ.put(SqlTypeName.SMALLINT, QueryDataType.SMALLINT);
        CALCITE_TO_HZ.put(SqlTypeName.INTEGER, QueryDataType.INT);
        CALCITE_TO_HZ.put(SqlTypeName.BIGINT, QueryDataType.BIGINT);

        HZ_TO_CALCITE.put(QueryDataTypeFamily.DECIMAL, SqlTypeName.DECIMAL);
        CALCITE_TO_HZ.put(SqlTypeName.DECIMAL, QueryDataType.DECIMAL);

        HZ_TO_CALCITE.put(QueryDataTypeFamily.REAL, SqlTypeName.REAL);
        HZ_TO_CALCITE.put(QueryDataTypeFamily.DOUBLE, SqlTypeName.DOUBLE);
        CALCITE_TO_HZ.put(SqlTypeName.REAL, QueryDataType.REAL);
        CALCITE_TO_HZ.put(SqlTypeName.DOUBLE, QueryDataType.DOUBLE);

        HZ_TO_CALCITE.put(QueryDataTypeFamily.TIME, SqlTypeName.TIME);
        HZ_TO_CALCITE.put(QueryDataTypeFamily.DATE, SqlTypeName.DATE);
        HZ_TO_CALCITE.put(QueryDataTypeFamily.TIMESTAMP, SqlTypeName.TIMESTAMP);
        HZ_TO_CALCITE.put(QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE, SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        HZ_TO_CALCITE.put(QueryDataTypeFamily.INTERVAL_YEAR_MONTH, SqlTypeName.INTERVAL_YEAR_MONTH);
        HZ_TO_CALCITE.put(QueryDataTypeFamily.INTERVAL_DAY_SECOND, SqlTypeName.INTERVAL_DAY_SECOND);
        CALCITE_TO_HZ.put(SqlTypeName.TIME, QueryDataType.TIME);
        CALCITE_TO_HZ.put(SqlTypeName.DATE, QueryDataType.DATE);
        CALCITE_TO_HZ.put(SqlTypeName.TIMESTAMP, QueryDataType.TIMESTAMP);
        CALCITE_TO_HZ.put(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME);
        CALCITE_TO_HZ.put(SqlTypeName.INTERVAL_YEAR_MONTH, QueryDataType.INTERVAL_YEAR_MONTH);
        CALCITE_TO_HZ.put(SqlTypeName.INTERVAL_DAY_SECOND, QueryDataType.INTERVAL_DAY_SECOND);

        // TODO: VO: Object type should be very restrictive, but currently all check for it are skipped. Use "STRUCTURED"?
        HZ_TO_CALCITE.put(QueryDataTypeFamily.OBJECT, SqlTypeName.ANY);
        CALCITE_TO_HZ.put(SqlTypeName.ANY, QueryDataType.OBJECT);

        HZ_TO_CALCITE.put(QueryDataTypeFamily.NULL, SqlTypeName.NULL);
        CALCITE_TO_HZ.put(SqlTypeName.NULL, QueryDataType.NULL);
    }

    private SqlToQueryType() {
        // No-op.
    }

    public static SqlTypeName map(QueryDataTypeFamily family) {
        return HZ_TO_CALCITE.get(family);
    }

    public static QueryDataType map(SqlTypeName sqlTypeName) {
        if (YEAR_INTERVAL_TYPES.contains(sqlTypeName)) {
            sqlTypeName = SqlTypeName.INTERVAL_YEAR_MONTH;
        } else if (DAY_INTERVAL_TYPES.contains(sqlTypeName)) {
            sqlTypeName = SqlTypeName.INTERVAL_DAY_SECOND;
        }

        QueryDataType queryDataType = CALCITE_TO_HZ.get(sqlTypeName);
        if (queryDataType == null) {
            throw new IllegalArgumentException("unexpected SQL type: " + sqlTypeName);
        }
        return queryDataType;
    }

    public static QueryDataType[] mapRowType(RelDataType rowType) {
        List<RelDataTypeField> fields = rowType.getFieldList();

        QueryDataType[] mappedRowType = new QueryDataType[fields.size()];
        for (int i = 0; i < fields.size(); ++i) {
            mappedRowType[i] = SqlToQueryType.map(fields.get(i).getType().getSqlTypeName());
        }
        return mappedRowType;
    }

}
