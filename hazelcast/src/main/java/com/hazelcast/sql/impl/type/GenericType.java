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

public enum GenericType {
    BIT(false),
    TINYINT(false),
    SMALLINT(false),
    INT(false),
    BIGINT(false),
    DECIMAL(false),
    REAL(false),
    DOUBLE(false),
    VARCHAR(false),
    DATE(true),
    TIME(true),
    TIMESTAMP(true),
    TIMESTAMP_WITH_TIMEZONE(true),
    INTERVAL_YEAR_MONTH(false),
    INTERVAL_DAY_SECOND(false),
    OBJECT(false),
    LATE(true);

    private final boolean temporal;

    GenericType(boolean temporal) {
        this.temporal = temporal;
    }

    public boolean isTemporal() {
        return temporal;
    }
}
