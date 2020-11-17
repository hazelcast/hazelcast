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

package com.hazelcast.sql.impl.type;

import com.hazelcast.sql.SqlColumnType;

import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.PRECEDENCE_BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.PRECEDENCE_BOOLEAN;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.PRECEDENCE_DATE;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.PRECEDENCE_DECIMAL;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.PRECEDENCE_DOUBLE;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.PRECEDENCE_INTEGER;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.PRECEDENCE_NULL;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.PRECEDENCE_OBJECT;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.PRECEDENCE_REAL;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.PRECEDENCE_SMALLINT;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.PRECEDENCE_TIME;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.PRECEDENCE_TIMESTAMP;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.PRECEDENCE_TIMESTAMP_WITH_TIME_ZONE;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.PRECEDENCE_TINYINT;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.PRECEDENCE_VARCHAR;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_DATE;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_NULL;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_TIMESTAMP;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_DECIMAL;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_OBJECT;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_TIME;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_TIMESTAMP_WITH_TIME_ZONE;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_VARCHAR;

public enum QueryDataTypeFamily {
    NULL(QueryDataTypeGroup.NULL, TYPE_LEN_NULL, PRECEDENCE_NULL, SqlColumnType.NULL),
    VARCHAR(QueryDataTypeGroup.VARCHAR, TYPE_LEN_VARCHAR, PRECEDENCE_VARCHAR, SqlColumnType.VARCHAR),
    BOOLEAN(QueryDataTypeGroup.BOOLEAN, 1, PRECEDENCE_BOOLEAN, SqlColumnType.BOOLEAN),
    TINYINT(QueryDataTypeGroup.NUMERIC, 1, PRECEDENCE_TINYINT, SqlColumnType.TINYINT),
    SMALLINT(QueryDataTypeGroup.NUMERIC, 2, PRECEDENCE_SMALLINT, SqlColumnType.SMALLINT),
    INTEGER(QueryDataTypeGroup.NUMERIC, 4, PRECEDENCE_INTEGER, SqlColumnType.INTEGER),
    BIGINT(QueryDataTypeGroup.NUMERIC, 8, PRECEDENCE_BIGINT, SqlColumnType.BIGINT),
    DECIMAL(QueryDataTypeGroup.NUMERIC, TYPE_LEN_DECIMAL, PRECEDENCE_DECIMAL, SqlColumnType.DECIMAL),
    REAL(QueryDataTypeGroup.NUMERIC, 4, PRECEDENCE_REAL, SqlColumnType.REAL),
    DOUBLE(QueryDataTypeGroup.NUMERIC, 8, PRECEDENCE_DOUBLE, SqlColumnType.DOUBLE),
    TIME(QueryDataTypeGroup.TIME, TYPE_LEN_TIME, PRECEDENCE_TIME, SqlColumnType.TIME),
    DATE(QueryDataTypeGroup.DATE, TYPE_LEN_DATE, PRECEDENCE_DATE, SqlColumnType.DATE),
    TIMESTAMP(QueryDataTypeGroup.TIMESTAMP, TYPE_LEN_TIMESTAMP, PRECEDENCE_TIMESTAMP, SqlColumnType.TIMESTAMP),
    TIMESTAMP_WITH_TIME_ZONE(
        QueryDataTypeGroup.TIMESTAMP_WITH_TIME_ZONE,
        TYPE_LEN_TIMESTAMP_WITH_TIME_ZONE,
        PRECEDENCE_TIMESTAMP_WITH_TIME_ZONE,
        SqlColumnType.TIMESTAMP_WITH_TIME_ZONE
    ),
    OBJECT(QueryDataTypeGroup.OBJECT, TYPE_LEN_OBJECT, PRECEDENCE_OBJECT, SqlColumnType.OBJECT);

    private final QueryDataTypeGroup group;
    private final int estimatedSize;
    private final int precedence;
    private final SqlColumnType publicType;

    QueryDataTypeFamily(QueryDataTypeGroup group, int estimatedSize, int precedence, SqlColumnType publictype) {
        assert estimatedSize > 0;

        this.group = group;
        this.estimatedSize = estimatedSize;
        this.precedence = precedence;
        this.publicType = publictype;
    }

    public QueryDataTypeGroup getGroup() {
        return group;
    }

    public boolean isNumeric() {
        return group == QueryDataTypeGroup.NUMERIC;
    }

    public boolean isTemporal() {
        switch (group) {
            case TIME:
            case DATE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIME_ZONE:
                return true;

            default:
                return false;
        }
    }

    public int getEstimatedSize() {
        return estimatedSize;
    }

    public int getPrecedence() {
        return precedence;
    }

    public SqlColumnType getPublicType() {
        return publicType;
    }
}
