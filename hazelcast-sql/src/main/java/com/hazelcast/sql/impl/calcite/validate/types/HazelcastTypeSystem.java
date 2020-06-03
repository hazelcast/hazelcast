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

package com.hazelcast.sql.impl.calcite.validate.types;

import com.hazelcast.internal.util.collection.Object2LongHashMap;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.calcite.SqlToQueryType;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.SqlDaySecondInterval;
import com.hazelcast.sql.impl.type.SqlYearMonthInterval;
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.sql.impl.type.converter.Converters;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.util.Calendar;

import static org.apache.calcite.sql.type.SqlTypeName.ANY;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;
import static org.apache.calcite.sql.type.SqlTypeName.CHAR_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.DATE;
import static org.apache.calcite.sql.type.SqlTypeName.DATETIME_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.DAY_INTERVAL_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.FRACTIONAL_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_DAY_SECOND;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_YEAR_MONTH;
import static org.apache.calcite.sql.type.SqlTypeName.INT_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.NULL;
import static org.apache.calcite.sql.type.SqlTypeName.NUMERIC_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.REAL;
import static org.apache.calcite.sql.type.SqlTypeName.SMALLINT;
import static org.apache.calcite.sql.type.SqlTypeName.TIME;
import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP;
import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
import static org.apache.calcite.sql.type.SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE;
import static org.apache.calcite.sql.type.SqlTypeName.TINYINT;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
import static org.apache.calcite.sql.type.SqlTypeName.YEAR_INTERVAL_TYPES;

public final class HazelcastTypeSystem extends RelDataTypeSystemImpl {

    public static final RelDataTypeSystem INSTANCE = new HazelcastTypeSystem();

    public static final int MAX_DECIMAL_PRECISION = QueryDataType.MAX_DECIMAL_PRECISION;
    public static final int MAX_DECIMAL_SCALE = MAX_DECIMAL_PRECISION;

    private static final int MILLISECONDS_PER_SECOND = 1_000;
    private static final int NANOSECONDS_PER_MILLISECOND = 1_000_000;

    // Order is important for precedence determination: types at the end of
    // the array have higher precedence.
    private static final SqlTypeName[] TYPE_NAMES =
            {NULL, VARCHAR, BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL, REAL, DOUBLE, INTERVAL_YEAR_MONTH,
                    INTERVAL_DAY_SECOND, TIME, DATE, TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE, ANY};

    private static final Object2LongHashMap<SqlTypeName> TYPE_TO_PRECEDENCE = new Object2LongHashMap<>(-1);

    static {
        for (int i = 0; i < TYPE_NAMES.length; ++i) {
            TYPE_TO_PRECEDENCE.put(TYPE_NAMES[i], i);
        }
    }

    private HazelcastTypeSystem() {
    }

    public static boolean canCast(RelDataType from, RelDataType to) {
        if (typeName(from) == NULL) {
            return true;
        }

        QueryDataType queryFrom = SqlToQueryType.map(typeName(from));
        QueryDataType queryTo = SqlToQueryType.map(typeName(to));
        return queryFrom.getConverter().canConvertTo(queryTo.getTypeFamily());
    }

    public static boolean canRepresent(SqlLiteral literal, RelDataType as) {
        return canConvert(literalValue(literal), literalType(literal), as);
    }

    public static boolean canConvert(Object value, RelDataType from, RelDataType to) {
        QueryDataType queryFrom = SqlToQueryType.map(typeName(from));
        QueryDataType queryTo = SqlToQueryType.map(typeName(to));

        Converter fromConverter = queryFrom.getConverter();
        Converter toConverter = queryTo.getConverter();

        if (!fromConverter.canConvertTo(queryTo.getTypeFamily())) {
            return false;
        }

        if (value == null) {
            return true;
        }

        Converter valueConverter = Converters.getConverter(value.getClass());

        // Convert literal value to 'from' type and then to 'to' type.

        Object fromValue;
        Object toValue;
        try {
            fromValue = fromConverter.convertToSelf(valueConverter, value);
            toValue = toConverter.convertToSelf(fromConverter, fromValue);
        } catch (QueryException e) {
            assert e.getCode() == SqlErrorCode.DATA_EXCEPTION;
            return false;
        }

        if (toValue instanceof BigDecimal) {
            // make sure the resulting decimal is valid
            BigDecimal numeric = (BigDecimal) toValue;
            return numeric.precision() <= HazelcastTypeSystem.MAX_DECIMAL_PRECISION;
        }

        return true;
    }

    public static RelDataType withHigherPrecedence(RelDataType type1, RelDataType type2) {
        int precedence1 = precedenceOf(type1);
        int precedence2 = precedenceOf(type2);
        assert precedence1 != precedence2 || type1.getSqlTypeName() == type2.getSqlTypeName();

        if (precedence1 == precedence2 && isInteger(type1) && isInteger(type2)) {
            int bitWidth1 = HazelcastIntegerType.bitWidthOf(type1);
            int bitWidth2 = HazelcastIntegerType.bitWidthOf(type2);
            return bitWidth1 > bitWidth2 ? type1 : type2;
        }

        return precedence1 > precedence2 ? type1 : type2;
    }

    public static RelDataType withHigherPrecedenceForLiterals(RelDataType type1, RelDataType type2) {
        if (typeName(type1) == DECIMAL && isNumeric(type2)) {
            return type1;
        }
        if (typeName(type2) == DECIMAL && isNumeric(type1)) {
            return type2;
        }

        return withHigherPrecedence(type1, type2);
    }

    public static RelDataType narrowestTypeFor(BigDecimal value, SqlTypeName otherType) {
        if (value.scale() <= 0) {
            try {
                long longValue = value.longValueExact();

                int bitWidth = HazelcastIntegerType.bitWidthOf(longValue);
                return HazelcastIntegerType.of(bitWidth, false);
            } catch (ArithmeticException e) {
                return HazelcastTypeFactory.INSTANCE.createSqlType(FRACTIONAL_TYPES.contains(otherType) ? otherType : BIGINT);
            }
        } else {
            return HazelcastTypeFactory.INSTANCE.createSqlType(FRACTIONAL_TYPES.contains(otherType) ? otherType : DOUBLE);
        }
    }

    public static SqlTypeName typeName(RelDataType type) {
        return type.getSqlTypeName();
    }

    public static boolean isNumeric(RelDataType type) {
        return NUMERIC_TYPES.contains(typeName(type));
    }

    public static boolean isChar(RelDataType type) {
        return CHAR_TYPES.contains(typeName(type));
    }

    public static boolean isFloatingPoint(RelDataType type) {
        return FRACTIONAL_TYPES.contains(typeName(type));
    }

    public static boolean isInteger(RelDataType type) {
        return INT_TYPES.contains(type.getSqlTypeName());
    }

    public static boolean isTemporal(RelDataType type) {
        return DATETIME_TYPES.contains(typeName(type)) || INTERVAL_TYPES.contains(typeName(type));
    }

    @Override
    public int getMaxNumericPrecision() {
        return MAX_DECIMAL_PRECISION;
    }

    @Override
    public int getMaxNumericScale() {
        return MAX_DECIMAL_SCALE;
    }

    private static int precedenceOf(RelDataType type) {
        long value = TYPE_TO_PRECEDENCE.getValue(type.getSqlTypeName());
        if (value == -1) {
            throw new IllegalArgumentException("unexpected type name: " + type.getSqlTypeName());
        }
        return (int) value;
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:ReturnCount"})
    private static Object literalValue(SqlLiteral literal) {
        switch (literal.getTypeName()) {
            case VARCHAR:
            case CHAR:
                return literal.getValueAs(String.class);

            case BOOLEAN:
                return literal.getValueAs(Boolean.class);

            case TINYINT:
                return literal.getValueAs(Byte.class);
            case SMALLINT:
                return literal.getValueAs(Short.class);
            case INTEGER:
                return literal.getValueAs(Integer.class);
            case BIGINT:
                return literal.getValueAs(Long.class);

            case DECIMAL:
                return literal.getValueAs(BigDecimal.class);

            case REAL:
                return literal.getValueAs(Float.class);
            case DOUBLE:
                return literal.getValueAs(Double.class);

            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
            case DATE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return literal.getValueAs(Calendar.class);

            case INTERVAL_YEAR:
            case INTERVAL_MONTH:
            case INTERVAL_YEAR_MONTH:
                long months = literal.getValueAs(Long.class);
                return new SqlYearMonthInterval((int) months);

            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                long milliseconds = literal.getValueAs(Long.class);
                long seconds = milliseconds / MILLISECONDS_PER_SECOND;
                int nanoseconds = (int) (milliseconds % MILLISECONDS_PER_SECOND) * NANOSECONDS_PER_MILLISECOND;
                return new SqlDaySecondInterval(seconds, nanoseconds);

            case ANY:
                return literal.getValueAs(Object.class);

            case NULL:
                return null;

            default:
                throw new IllegalArgumentException("unexpected literal type: " + literal.getTypeName());
        }
    }

    private static RelDataType literalType(SqlLiteral literal) {
        if (YEAR_INTERVAL_TYPES.contains(literal.getTypeName())) {
            return HazelcastTypeFactory.INSTANCE.createSqlType(INTERVAL_YEAR_MONTH);
        }
        if (DAY_INTERVAL_TYPES.contains(literal.getTypeName())) {
            return HazelcastTypeFactory.INSTANCE.createSqlType(INTERVAL_DAY_SECOND);
        }

        if (literal.getTypeName() == TIME_WITH_LOCAL_TIME_ZONE) {
            return HazelcastTypeFactory.INSTANCE.createSqlType(TIME);
        }

        return HazelcastTypeFactory.INSTANCE.createSqlType(literal.getTypeName());
    }

}
