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

package com.hazelcast.sql.impl.expression;

import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

/**
 * Generic expression.
 *
 * @param <T> Return type.
 */
// TODO: Move all these virtually static methods to a separate utility class.
public interface Expression<T> extends DataSerializable {
    /**
     * Evaluate the expression.
     *
     * @param row Row.
     * @return Result.
     */
    T eval(Row row);

    default Boolean evalAsBit(Row row) {
        Object res = eval(row);

        if (res == null) {
            return null;
        }

        return getType().getConverter().asBit(res);
    }

    default Integer evalAsInt(Row row) {
        Object res = eval(row);

        if (res == null) {
            return null;
        }

        return getType().getConverter().asInt(res);
    }

    default Long evalAsBigint(Row row) {
        Object res = eval(row);

        if (res == null) {
            return null;
        }

        return getType().getConverter().asBigint(res);
    }

    default BigDecimal evalAsDecimal(Row row) {
        Object res = eval(row);

        if (res == null) {
            return null;
        }

        return getType().getConverter().asDecimal(res);
    }

    default Double evalAsDouble(Row row) {
        Object res = eval(row);

        if (res == null) {
            return null;
        }

        return getType().getConverter().asDouble(res);
    }

    default String evalAsVarchar(Row row) {
        Object res = eval(row);

        if (res == null) {
            return null;
        }

        return getType().getConverter().asVarchar(res);
    }

    default OffsetDateTime evalAsTimestampWithTimezone(Row row) {
        Object res = eval(row);

        if (res == null) {
            return null;
        }

        return getType().getConverter().asTimestampWithTimezone(res);
    }

    default boolean canConvertToBit() {
        return getType().getConverter().canConvertToBit();
    }

    default boolean canConvertToInt() {
        return getType().getConverter().canConvertToInt();
    }

    default boolean canConvertToVarchar() {
        return getType().getConverter().canConvertToVarchar();
    }

    default boolean canConvertToTimestampWithTimezone() {
        return getType().getConverter().canConvertToVarchar();
    }

    default void ensureCanConvertToBit() {
        if (!canConvertToBit()) {
            throw HazelcastSqlException.error("Expression cannot be converted to BIT: " + this);
        }
    }

    default void ensureCanConvertToInt() {
        if (!canConvertToInt()) {
            throw HazelcastSqlException.error("Expression cannot be converted to INT: " + this);
        }
    }

    default void ensureCanConvertToVarchar() {
        if (!canConvertToVarchar()) {
            throw HazelcastSqlException.error("Expression cannot be converted to VARCHAR: " + this);
        }
    }

    default void ensureCanConvertToTimestampWithTimezone() {
        if (!canConvertToTimestampWithTimezone()) {
            throw HazelcastSqlException.error("Expression cannot be converted to TIMESTAMP_WITH_TIMEZONE: " + this);
        }
    }

    /**
     * @return Return type of the expression.
     */
    DataType getType();
}
