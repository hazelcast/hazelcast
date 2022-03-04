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

package com.hazelcast.sql;

import com.hazelcast.core.HazelcastJsonValue;

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
    VARCHAR(0, String.class),

    /** BOOLEAN type, represented by {@link java.lang.Boolean} */
    BOOLEAN(1, Boolean.class),

    /** TINYINT type, represented by {@link java.lang.Byte} */
    TINYINT(2, Byte.class),

    /** SMALLINT type, represented by {@link java.lang.Short} */
    SMALLINT(3, Short.class),

    /** INTEGER type, represented by {@link java.lang.Integer} */
    INTEGER(4, Integer.class),

    /** BIGINT type, represented by {@link java.lang.Long} */
    BIGINT(5, Long.class),

    /** DECIMAL type, represented by {@link java.math.BigDecimal} */
    DECIMAL(6, BigDecimal.class),

    /** REAL type, represented by {@link java.lang.Float} */
    REAL(7, Float.class),

    /** DOUBLE type, represented by {@link java.lang.Double} */
    DOUBLE(8, Double.class),

    /** DATE type, represented by {@link java.time.LocalDate} */
    DATE(9, LocalDate.class),

    /** TIME type, represented by {@link java.time.LocalTime} */
    TIME(10, LocalTime.class),

    /** TIMESTAMP type, represented by {@link java.time.LocalDateTime} */
    TIMESTAMP(11, LocalDateTime.class),

    /** TIMESTAMP_WITH_TIME_ZONE type, represented by {@link java.time.OffsetDateTime} */
    TIMESTAMP_WITH_TIME_ZONE(12, OffsetDateTime.class),

    /** OBJECT type, could be represented by any Java class. */
    OBJECT(13, Object.class),

    /**
     * The type of the generic SQL {@code NULL} literal.
     * <p>
     * The only valid value of {@code NULL} type is {@code null}.
     */
    NULL(14, Void.class),

    /** JSON type, represented by {@link HazelcastJsonValue} */
    JSON(15, HazelcastJsonValue.class);

    private static final SqlColumnType[] CACHED_VALUES = values();

    private final int id;
    private final Class<?> valueClass;

    SqlColumnType(int id, Class<?> valueClass) {
        this.id = id;
        this.valueClass = valueClass;
    }

    /**
     * Returns the IndexType as an enum.
     *
     * @return the IndexType as an enum
     */
    public static SqlColumnType getById(final int id) {
        for (SqlColumnType type : CACHED_VALUES) {
            if (type.id == id) {
                return type;
            }
        }

        return null;
    }

    /**
     * Gets the ID for the given {@link SqlColumnType}.
     *
     * @return the ID
     */
    public int getId() {
        return id;
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

    @Override
    public String toString() {
        return name().replace('_', ' ');
    }
}
