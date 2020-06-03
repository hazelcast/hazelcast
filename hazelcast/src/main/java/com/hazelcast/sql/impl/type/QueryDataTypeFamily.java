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

import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_INTERVAL_DAY_SECOND;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_INTERVAL_YEAR_MONTH;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_DATE;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_NULL;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_TIMESTAMP;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_DECIMAL;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_OBJECT;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_TIME;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_TIMESTAMP_WITH_OFFSET;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.TYPE_LEN_VARCHAR;

public enum QueryDataTypeFamily {
    NULL(false, TYPE_LEN_NULL),
    VARCHAR(false, TYPE_LEN_VARCHAR),
    BOOLEAN(false, 1),
    TINYINT(false, 1),
    SMALLINT(false, 2),
    INT(false, 4),
    BIGINT(false, 8),
    DECIMAL(false, TYPE_LEN_DECIMAL),
    REAL(false, 4),
    DOUBLE(false, 8),
    INTERVAL_YEAR_MONTH(true, TYPE_INTERVAL_YEAR_MONTH),
    INTERVAL_DAY_SECOND(true, TYPE_INTERVAL_DAY_SECOND),
    TIME(true, TYPE_LEN_TIME),
    DATE(true, TYPE_LEN_DATE),
    TIMESTAMP(true, TYPE_LEN_TIMESTAMP),
    TIMESTAMP_WITH_TIME_ZONE(true, TYPE_LEN_TIMESTAMP_WITH_OFFSET),
    OBJECT(false, TYPE_LEN_OBJECT);

    private final boolean temporal;
    private final int estimatedSize;

    QueryDataTypeFamily(boolean temporal, int estimatedSize) {
        assert estimatedSize > 0;

        this.temporal = temporal;
        this.estimatedSize = estimatedSize;
    }

    public boolean isTemporal() {
        return temporal;
    }

    public int getEstimatedSize() {
        return estimatedSize;
    }
}
