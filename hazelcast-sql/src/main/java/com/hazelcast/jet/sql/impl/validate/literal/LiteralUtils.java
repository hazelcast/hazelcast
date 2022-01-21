/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.validate.literal;

import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeFactory;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimeString;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.temporal.Temporal;
import java.util.Calendar;

import static org.apache.calcite.sql.type.SqlTypeName.CHAR_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.DATETIME_TYPES;

public final class LiteralUtils {
    private static final long NANOSECOND_IN_MILLISECOND = 1_000_000L;

    private LiteralUtils() {
        // No-op
    }

    public static Literal literal(RexNode node) {
        if (node.getKind() != SqlKind.LITERAL) {
            // Not a literal
            return null;
        }

        RexLiteral literal = (RexLiteral) node;
        SqlTypeName typeName = literal.getTypeName();

        // Intercept DATE, TIME, TIMESTAMP types to return (Offset, Local)(Date, Time) instead of Calendar class
        Object value = (DATETIME_TYPES.contains(typeName) && typeName != SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE)
                ? createDateTimeValue(typeName, literal)
                : literal.getValue();

        return literal0(node.getType().getSqlTypeName(), value);
    }

    public static Literal literal(SqlNode node) {
        if (node.getKind() != SqlKind.LITERAL) {
            // Not a literal
            return null;
        }

        SqlLiteral literal = (SqlLiteral) node;
        SqlTypeName typeName = literal.getTypeName();

        Object value = CHAR_TYPES.contains(typeName) ? literal.toValue() : literal.getValue();
        return literal0(typeName, value);
    }

    private static Literal literal0(SqlTypeName typeName, Object value) {
        // Do no convert symbols.
        if (typeName == SqlTypeName.SYMBOL) {
            return null;
        }

        if (HazelcastTypeUtils.isNumericType(typeName)) {
            return NumericLiteral.create(typeName, value);
        }

        if (CHAR_TYPES.contains(typeName)) {
            if (value instanceof NlsString) {
                value = ((NlsString) value).getValue();
            }
            assert value instanceof String : value.getClass().getName();
            return new TypedLiteral(value, SqlTypeName.VARCHAR);
        }

        if (value instanceof SqlIntervalLiteral.IntervalValue) {
            return new IntervalLiteral((SqlIntervalLiteral.IntervalValue) value, typeName);
        }

        return new TypedLiteral(value, typeName);
    }

    public static SqlTypeName literalTypeName(SqlNode node) {
        Literal literal = literal(node);

        return literal != null ? literal.getTypeName() : null;
    }

    public static RelDataType literalType(SqlNode node, HazelcastTypeFactory typeFactory) {
        Literal literal = literal(node);

        return literal != null ? literal.getType(typeFactory) : null;
    }

    private static Temporal createDateTimeValue(SqlTypeName typeName, RexLiteral literal) {
        Calendar calendar;
        switch (typeName) {
            case TIME:
                return LocalTime.ofNanoOfDay(
                                literal.getValueAs(TimeString.class).getMillisOfDay() * NANOSECOND_IN_MILLISECOND
                        );
            case TIMESTAMP:
                calendar = (Calendar) literal.getValue();
                return LocalDateTime.ofInstant(calendar.toInstant(), calendar.getTimeZone().toZoneId());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                calendar = (Calendar) literal.getValue();
                return OffsetDateTime.ofInstant(calendar.toInstant(), calendar.getTimeZone().toZoneId());
            case DATE:
                return LocalDate.ofEpochDay(literal.getValueAs(Integer.class));
            default:
                throw new IllegalArgumentException("Unexpected type: " + typeName);
        }
    }
}
