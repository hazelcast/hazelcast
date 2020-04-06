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

package com.hazelcast.sql;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

public enum SqlColumnType {
    /** VARCHAR type. */
    VARCHAR(String.class),

    /** BOOLEAN type. */
    BOOLEAN(Boolean.class),

    /** TINYINT type. */
    TINYINT(Short.class),

    /** SMALLINT type. */
    SMALLINT(Short.class),

    /** INT type. */
    INT(Integer.class),

    /** BIGINT type. */
    BIGINT(Long.class),

    /** DECIMAL type. */
    DECIMAL(BigDecimal.class),

    /** REAL type. */
    REAL(Float.class),

    /** DOUBLE type. */
    DOUBLE(Double.class),

    /** DATE type. */
    DATE(LocalDate.class),

    /** TIME type. */
    TIME(LocalTime.class),

    /** TIMESTAMP type. */
    TIMESTAMP(LocalDateTime.class),

    /** TIMESTAMP_WITH_TIME_ZONE type. */
    TIMESTAMP_WITH_TIME_ZONE(OffsetDateTime.class),

    /** OBJECT type. */
    OBJECT(Object.class);

    private final Class<?> valueClass;

    SqlColumnType(Class<?> valueClass) {
        this.valueClass = valueClass;
    }

    public Class<?> getValueClass() {
        return valueClass;
    }
}
