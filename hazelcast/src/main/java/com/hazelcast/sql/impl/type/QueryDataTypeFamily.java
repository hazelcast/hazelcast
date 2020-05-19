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

import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_DATE;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_DECIMAL;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_OBJECT;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_TIME;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_TIMESTAMP;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_TIMESTAMP_WITH_OFFSET;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_VARCHAR;

public enum QueryDataTypeFamily {
    LATE(false, 0, TYPE_LEN_OBJECT),
    VARCHAR(false, 100, TYPE_LEN_VARCHAR),
    BOOLEAN(false, 200, 1),
    TINYINT(false, 300, 1),
    SMALLINT(false, 400, 2),
    INT(false, 500, 4),
    BIGINT(false, 600, 8),
    DECIMAL(false, 700, TYPE_LEN_DECIMAL),
    REAL(false, 800, 4),
    DOUBLE(false, 900, 8),
    TIME(true, 1000, TYPE_LEN_TIME),
    DATE(true, 1100, TYPE_LEN_DATE),
    TIMESTAMP(true, 1200, TYPE_LEN_TIMESTAMP),
    TIMESTAMP_WITH_TIME_ZONE(true, 1300, TYPE_LEN_TIMESTAMP_WITH_OFFSET),
    OBJECT(false, 1400, TYPE_LEN_OBJECT);

    private final boolean temporal;
    private final int precedence;
    private final int estimatedSize;

    QueryDataTypeFamily(boolean temporal, int precedence, int estimatedSize) {
        assert precedence >= 0;
        assert estimatedSize > 0;

        this.temporal = temporal;
        this.precedence = precedence;
        this.estimatedSize = estimatedSize;
    }

    public boolean isTemporal() {
        return temporal;
    }

    public int getPrecedence() {
        return precedence;
    }

    public int getEstimatedSize() {
        return estimatedSize;
    }
}
