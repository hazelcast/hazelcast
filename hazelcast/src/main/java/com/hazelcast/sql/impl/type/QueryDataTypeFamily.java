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
    NULL(false, TYPE_LEN_NULL, PRECEDENCE_NULL),
    VARCHAR(false, TYPE_LEN_VARCHAR, PRECEDENCE_VARCHAR),
    BOOLEAN(false, 1, PRECEDENCE_BOOLEAN),
    TINYINT(false, 1, PRECEDENCE_TINYINT),
    SMALLINT(false, 2, PRECEDENCE_SMALLINT),
    INTEGER(false, 4, PRECEDENCE_INTEGER),
    BIGINT(false, 8, PRECEDENCE_BIGINT),
    DECIMAL(false, TYPE_LEN_DECIMAL, PRECEDENCE_DECIMAL),
    REAL(false, 4, PRECEDENCE_REAL),
    DOUBLE(false, 8, PRECEDENCE_DOUBLE),
    TIME(true, TYPE_LEN_TIME, PRECEDENCE_TIME),
    DATE(true, TYPE_LEN_DATE, PRECEDENCE_DATE),
    TIMESTAMP(true, TYPE_LEN_TIMESTAMP, PRECEDENCE_TIMESTAMP),
    TIMESTAMP_WITH_TIME_ZONE(true, TYPE_LEN_TIMESTAMP_WITH_TIME_ZONE, PRECEDENCE_TIMESTAMP_WITH_TIME_ZONE),
    OBJECT(false, TYPE_LEN_OBJECT, PRECEDENCE_OBJECT);

    private final boolean temporal;
    private final int estimatedSize;
    private final int precedence;

    QueryDataTypeFamily(boolean temporal, int estimatedSize, int precedence) {
        assert estimatedSize > 0;

        this.temporal = temporal;
        this.estimatedSize = estimatedSize;
        this.precedence = precedence;
    }

    public boolean isTemporal() {
        return temporal;
    }

    public int getEstimatedSize() {
        return estimatedSize;
    }

    public int getPrecedence() {
        return precedence;
    }
}
