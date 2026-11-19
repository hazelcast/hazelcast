/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.sql.impl.opt.physical.visitor;

import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

final class ConversionUtil {

    private ConversionUtil() {
    }

    static Comparable<?> convertSargValue(BigDecimal value, SqlTypeName typeName) {
        return switch (typeName) {
            case TINYINT -> value.byteValueExact();
            case SMALLINT -> value.shortValueExact();
            case INTEGER -> value.intValueExact();
            case BIGINT -> value.longValueExact();
            case REAL, FLOAT -> value.floatValue();
            case DOUBLE -> value.doubleValue();
            default -> throw new IllegalArgumentException("Cannot convert " + value.getClass());
        };
    }

    static Comparable<?> convertSargValue(Number value, SqlTypeName typeName) {
        return switch (typeName) {
            case TINYINT -> value.byteValue();
            case SMALLINT -> value.shortValue();
            case INTEGER -> value.intValue();
            case BIGINT -> value.longValue();
            case REAL, FLOAT -> value.floatValue();
            case DOUBLE -> value.doubleValue();
            default -> throw new IllegalArgumentException("Cannot convert " + value.getClass());
        };
    }

    static Expression<?> convertBooleanLiteral(RexLiteral literal, RelDataType type) {
        assert type.getSqlTypeName() == SqlTypeName.BOOLEAN;
        Boolean value = literal.getValueAs(Boolean.class);
        return ConstantExpression.create(value, HazelcastTypeUtils.toHazelcastType(type));
    }

    static LocalTime toLocalTime(TimeString string) {
        try {
            return LocalTime.parse(string.toString());
        } catch (Exception e) {
            throw QueryException.dataException("Cannot convert literal to " + SqlColumnType.TIME + ": " + string);
        }
    }

    static LocalDate toLocalDate(DateString string) {
        try {
            return LocalDate.parse(string.toString());
        } catch (Exception e) {
            throw QueryException.dataException("Cannot convert literal to " + SqlColumnType.DATE + ": " + string);
        }
    }

    static LocalDateTime toLocalDateTime(TimestampString string) {
        try {
            return LocalDateTime.parse(string.toString().replace(' ', 'T'));
        } catch (Exception e) {
            throw QueryException.dataException("Cannot convert literal to " + SqlColumnType.TIMESTAMP + ": " + string);
        }
    }
}
