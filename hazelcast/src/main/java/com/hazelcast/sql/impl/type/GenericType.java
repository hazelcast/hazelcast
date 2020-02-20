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

// TODO: Review size estimations.
public enum GenericType {
    BIT(false, 1),
    TINYINT(false, 1),
    SMALLINT(false, 2),
    INT(false, 4),
    BIGINT(false, 8),
    DECIMAL(false, 32),
    REAL(false, 4),
    DOUBLE(false, 8),
    VARCHAR(false, 32),
    DATE(true, 8),
    TIME(true, 8),
    TIMESTAMP(true, 16),
    TIMESTAMP_WITH_TIMEZONE(true, 24),
    INTERVAL_YEAR_MONTH(false, 8),
    INTERVAL_DAY_SECOND(false, 8),
    OBJECT(false, 48),
    LATE(true, 48);

    private final boolean temporal;
    private final int estimatedSize;

    GenericType(boolean temporal, int estimatedSize) {
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
