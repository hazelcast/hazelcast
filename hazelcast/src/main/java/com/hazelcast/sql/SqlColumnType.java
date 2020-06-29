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

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

/**
 * SQL column type.
 */
public enum SqlColumnType {
    /** VARCHAR type, represented by {@link java.lang.String} */
    VARCHAR(String.class),

    /** BOOLEAN type, represented by {@link java.lang.Boolean} */
    BOOLEAN(Boolean.class),

    /** TINYINT type, represented by {@link java.lang.Byte} */
    TINYINT(Byte.class),

    /** SMALLINT type, represented by {@link java.lang.Short} */
    SMALLINT(Short.class),

    /** INT type, represented by {@link java.lang.Integer} */
    INT(Integer.class),

    /** BIGINT type, represented by {@link java.lang.Long} */
    BIGINT(Long.class),

    /** DECIMAL type, represented by {@link java.math.BigDecimal} */
    DECIMAL(BigDecimal.class),

    /** REAL type, represented by {@link java.lang.Float} */
    REAL(Float.class),

    /** DOUBLE type, represented by {@link java.lang.Double} */
    DOUBLE(Double.class),

    /** DATE type, represented by {@link java.time.LocalDate} */
    DATE(LocalDate.class),

    /** TIME type, represented by {@link java.time.LocalTime} */
    TIME(LocalTime.class),

    /** TIMESTAMP type,, represented by {@link java.time.LocalDateTime} */
    TIMESTAMP(LocalDateTime.class),

    /** TIMESTAMP_WITH_TIME_ZONE type, represented by {@link java.time.OffsetDateTime} */
    TIMESTAMP_WITH_TIME_ZONE(OffsetDateTime.class),

    /** OBJECT type, could be represented by any Java class. */
    OBJECT(Object.class);

    private final Class<?> valueClass;

    SqlColumnType(Class<?> valueClass) {
        this.valueClass = valueClass;
    }

    /**
     * Gets the Java class of the value of this SQL type.
     *
     * @return the Java class of the value of this SQL type
     */
    @Nonnull
    public Class<?> getValueClass() {
        return valueClass;
    }
}
