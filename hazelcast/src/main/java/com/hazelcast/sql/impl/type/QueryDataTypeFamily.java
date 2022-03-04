/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.PRECEDENCE_INTERVAL_DAY_SECOND;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.PRECEDENCE_INTERVAL_YEAR_MONTH;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.PRECEDENCE_JSON;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.PRECEDENCE_MAP;
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
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_DECIMAL;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_INTERVAL_DAY_SECOND;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_INTERVAL_YEAR_MONTH;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_JSON;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_MAP;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_NULL;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_OBJECT;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_TIME;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_TIMESTAMP;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_TIMESTAMP_WITH_TIME_ZONE;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_VARCHAR;

public enum QueryDataTypeFamily {
    NULL(TYPE_LEN_NULL, PRECEDENCE_NULL, SqlColumnType.NULL),
    VARCHAR(TYPE_LEN_VARCHAR, PRECEDENCE_VARCHAR, SqlColumnType.VARCHAR),
    BOOLEAN(1, PRECEDENCE_BOOLEAN, SqlColumnType.BOOLEAN),
    TINYINT(1, PRECEDENCE_TINYINT, SqlColumnType.TINYINT),
    SMALLINT(2, PRECEDENCE_SMALLINT, SqlColumnType.SMALLINT),
    INTEGER(4, PRECEDENCE_INTEGER, SqlColumnType.INTEGER),
    BIGINT(8, PRECEDENCE_BIGINT, SqlColumnType.BIGINT),
    DECIMAL(TYPE_LEN_DECIMAL, PRECEDENCE_DECIMAL, SqlColumnType.DECIMAL),
    REAL(4, PRECEDENCE_REAL, SqlColumnType.REAL),
    DOUBLE(8, PRECEDENCE_DOUBLE, SqlColumnType.DOUBLE),
    TIME(TYPE_LEN_TIME, PRECEDENCE_TIME, SqlColumnType.TIME),
    DATE(TYPE_LEN_DATE, PRECEDENCE_DATE, SqlColumnType.DATE),
    TIMESTAMP(TYPE_LEN_TIMESTAMP, PRECEDENCE_TIMESTAMP, SqlColumnType.TIMESTAMP),
    TIMESTAMP_WITH_TIME_ZONE(
        TYPE_LEN_TIMESTAMP_WITH_TIME_ZONE,
        PRECEDENCE_TIMESTAMP_WITH_TIME_ZONE,
        SqlColumnType.TIMESTAMP_WITH_TIME_ZONE
    ),
    OBJECT(TYPE_LEN_OBJECT, PRECEDENCE_OBJECT, SqlColumnType.OBJECT),
    INTERVAL_YEAR_MONTH(TYPE_LEN_INTERVAL_YEAR_MONTH, PRECEDENCE_INTERVAL_YEAR_MONTH, null),
    INTERVAL_DAY_SECOND(TYPE_LEN_INTERVAL_DAY_SECOND, PRECEDENCE_INTERVAL_DAY_SECOND, null),
    MAP(TYPE_LEN_MAP, PRECEDENCE_MAP, null),
    JSON(TYPE_LEN_JSON, PRECEDENCE_JSON, SqlColumnType.JSON);

    private final int estimatedSize;
    private final int precedence;
    private final SqlColumnType publicType;

    QueryDataTypeFamily(int estimatedSize, int precedence, SqlColumnType publicType) {
        this.estimatedSize = estimatedSize;
        this.precedence = precedence;
        this.publicType = publicType;
    }

    public boolean isNumeric() {
        switch (this) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DECIMAL:
            case REAL:
            case DOUBLE:
                return true;

            default:
                return false;
        }
    }

    public boolean isNumericInteger() {
        switch (this) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return true;

            default:
                return false;
        }
    }

    public boolean isNumericApproximate() {
        switch (this) {
            case REAL:
            case DOUBLE:
                return true;

            default:
                return false;
        }
    }

    public boolean isTemporal() {
        switch (this) {
            case TIME:
            case DATE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIME_ZONE:
                return true;

            default:
                return false;
        }
    }

    public boolean isObject() {
        return this == QueryDataTypeFamily.OBJECT;
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

    @Override
    public String toString() {
        return name().replace('_', ' ');
    }
}
