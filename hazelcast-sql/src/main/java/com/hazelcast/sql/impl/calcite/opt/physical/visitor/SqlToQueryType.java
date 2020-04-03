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

package com.hazelcast.sql.impl.calcite.opt.physical.visitor;

import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Provides utilities to map from Calcite's {@link SqlTypeName} to {@link
 * QueryDataType}.
 */
public final class SqlToQueryType {

    private static final Map<SqlTypeName, QueryDataType> SQL_TO_QUERY_TYPE = new HashMap<>();

    static {
        SQL_TO_QUERY_TYPE.put(SqlTypeName.VARCHAR, QueryDataType.VARCHAR);
        SQL_TO_QUERY_TYPE.put(SqlTypeName.CHAR, QueryDataType.VARCHAR);

        SQL_TO_QUERY_TYPE.put(SqlTypeName.BOOLEAN, QueryDataType.BIT);

        SQL_TO_QUERY_TYPE.put(SqlTypeName.TINYINT, QueryDataType.TINYINT);
        SQL_TO_QUERY_TYPE.put(SqlTypeName.SMALLINT, QueryDataType.SMALLINT);
        SQL_TO_QUERY_TYPE.put(SqlTypeName.INTEGER, QueryDataType.INT);
        SQL_TO_QUERY_TYPE.put(SqlTypeName.BIGINT, QueryDataType.BIGINT);

        SQL_TO_QUERY_TYPE.put(SqlTypeName.DECIMAL, QueryDataType.DECIMAL);

        SQL_TO_QUERY_TYPE.put(SqlTypeName.REAL, QueryDataType.REAL);
        SQL_TO_QUERY_TYPE.put(SqlTypeName.DOUBLE, QueryDataType.DOUBLE);

        SQL_TO_QUERY_TYPE.put(SqlTypeName.TIME, QueryDataType.TIME);
        SQL_TO_QUERY_TYPE.put(SqlTypeName.DATE, QueryDataType.DATE);
        SQL_TO_QUERY_TYPE.put(SqlTypeName.TIMESTAMP, QueryDataType.TIMESTAMP);
        SQL_TO_QUERY_TYPE.put(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME);
        SQL_TO_QUERY_TYPE.put(SqlTypeName.INTERVAL_YEAR_MONTH, QueryDataType.INTERVAL_YEAR_MONTH);
        SQL_TO_QUERY_TYPE.put(SqlTypeName.INTERVAL_DAY_SECOND, QueryDataType.INTERVAL_DAY_SECOND);

        SQL_TO_QUERY_TYPE.put(SqlTypeName.ANY, QueryDataType.OBJECT);

        SQL_TO_QUERY_TYPE.put(SqlTypeName.NULL, QueryDataType.NULL);
    }

    private SqlToQueryType() {
    }

    /**
     * Maps the given {@link SqlTypeName} to {@link QueryDataType}.
     */
    public static QueryDataType map(SqlTypeName sqlTypeName) {
        QueryDataType queryDataType = SQL_TO_QUERY_TYPE.get(sqlTypeName);
        if (queryDataType == null) {
            throw new IllegalArgumentException("unexpected SQL type: " + sqlTypeName);
        }
        return queryDataType;
    }

    /**
     * Maps the given Calcite's row type to {@link QueryDataType} row type.
     */
    public static QueryDataType[] mapRowType(RelDataType rowType) {
        List<RelDataTypeField> fields = rowType.getFieldList();

        QueryDataType[] mappedRowType = new QueryDataType[fields.size()];
        for (int i = 0; i < fields.size(); ++i) {
            mappedRowType[i] = SqlToQueryType.map(fields.get(i).getType().getSqlTypeName());
        }
        return mappedRowType;
    }

}
