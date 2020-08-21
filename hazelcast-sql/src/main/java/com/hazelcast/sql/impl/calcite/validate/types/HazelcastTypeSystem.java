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

import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.calcite.SqlToQueryType;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.sql.impl.type.converter.Converters;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.util.Calendar;

import static org.apache.calcite.sql.type.SqlTypeName.APPROX_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.CHAR_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.DATETIME_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.DAY_INTERVAL_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.FRACTIONAL_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_DAY_SECOND;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_YEAR_MONTH;
import static org.apache.calcite.sql.type.SqlTypeName.INT_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.NUMERIC_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.YEAR_INTERVAL_TYPES;

/**
 * Custom Hazelcast type system.
 * <p>
 * Overrides some properties of the default Calcite type system, like maximum
 * numeric precision, and provides various type-related utilities for {@link
 * HazelcastTypeCoercion} and {@link HazelcastSqlValidator}.
 */
public final class HazelcastTypeSystem extends RelDataTypeSystemImpl {

    /**
     * Shared Hazelcast type system instance.
     */
    public static final RelDataTypeSystem INSTANCE = new HazelcastTypeSystem();

    /**
     * Defines maximum DECIMAL precision.
     */
    public static final int MAX_DECIMAL_PRECISION = QueryDataType.MAX_DECIMAL_PRECISION;

    /**
     * Defines maximum DECIMAL scale.
     */
    public static final int MAX_DECIMAL_SCALE = MAX_DECIMAL_PRECISION;

    /**
     * The name of Hazelcast OBJECT type.
     */
    public static final String OBJECT_TYPE_NAME = "OBJECT";

    private HazelcastTypeSystem() {
        // No-op
    }

    /**
     * @return {@code true} if the given identifier specifies OBJECT type,
     * {@code false} otherwise.
     */
    public static boolean isObject(SqlIdentifier identifier) {
        return identifier.isSimple() && OBJECT_TYPE_NAME.equalsIgnoreCase(identifier.getSimple());
    }

    /**
     * Determines is it possible to cast from one type to another or not.
     *
     * @param from the type to cast from.
     * @param to   the type to cast to.
     * @return {@code true} if the cast is possible, {@code false} otherwise.
     */
    public static boolean canCast(RelDataType from, RelDataType to) {
        QueryDataType queryFrom = SqlToQueryType.map(typeName(from));
        QueryDataType queryTo = SqlToQueryType.map(typeName(to));
        return queryFrom.getConverter().canConvertTo(queryTo.getTypeFamily());
    }

    /**
     * Determines is it possible to represent the given literal as a value of
     * the given target type.
     *
     * @param literal the literal in question.
     * @param target  the target type to represent the literal value as.
     * @return {@code true} if the type of given literal is compatible with
     * the given target type and the value of given literal is convertible to
     * the given target type, {@code false} otherwise.
     */
    public static boolean canRepresent(SqlLiteral literal, RelDataType target) {
        return canConvert(literalValue(literal), literalType(literal), target);
    }

    /**
     * Determines is it possible to convert the given value from its given type
     * to another type.
     *
     * @param value the value in question.
     * @param from  the type of the value.
     * @param to    the target type to convert to.
     * @return {@code true} if the type of given value is compatible with
     * the given target type and the value is convertible to the given target
     * type, {@code false} otherwise.
     */
    public static boolean canConvert(Object value, RelDataType from, RelDataType to) {
        QueryDataType queryFrom = SqlToQueryType.map(typeName(from));
        QueryDataType queryTo = SqlToQueryType.map(typeName(to));

        Converter fromConverter = queryFrom.getConverter();
        Converter toConverter = queryTo.getConverter();

        if (!fromConverter.canConvertTo(queryTo.getTypeFamily())) {
            return false;
        }

        if (value == null) {
            // nulls are convertible to any type
            return true;
        }

        Converter valueConverter = Converters.getConverter(value.getClass());

        // Convert literal value to 'from' type and then to 'to' type.

        Object fromValue;
        try {
            fromValue = fromConverter.convertToSelf(valueConverter, value);
            toConverter.convertToSelf(fromConverter, fromValue);
        } catch (QueryException e) {
            assert e.getCode() == SqlErrorCode.DATA_EXCEPTION;
            return false;
        }

        return true;
    }

    /**
     * Selects a type having a higher precedence from the two given types.
     * <p>
     * Type precedence is used to determine resulting types of operators and
     * to assign types to their operands.
     *
     * @param type1 the first type.
     * @param type2 the second type.
     * @return the type with the higher precedence.
     */
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

    /**
     * Selects the narrowest possible numeric type for the given {@link
     * Number} value with a potential fallback to the given other type if
     * that given other type is a floating-point type.
     * <p>
     * The given {@link Number} value can be either {@link BigDecimal}, in this
     * case the value is considered to be exact (see {@link
     * SqlTypeName#EXACT_TYPES}), or {@link Double}, in this case the value is
     * considered to be approximate (see {@link SqlTypeName#APPROX_TYPES}).
     * <p>
     * If the given value is exact, the narrowest integer type is selected if the
     * value can be represented without losses as TINYINT, SMALLINT, INTEGER or
     * BIGINT. Otherwise, the given fallback type is selected if it's a
     * floating-point type. If the given fallback type is not a floating-point
     * type, BIGINT is selected for integer values and DECIMAL is selected for
     * floating-point values.
     * <p>
     * If the given value is approximate, the given fallback type is selected if
     * the type is REAL; otherwise, DOUBLE is selected.
     * <p>
     * The method performs only the narrowest type selection and doesn't
     * validate the value itself.
     *
     * @param value     the value to select the narrowest type for.
     * @param otherType the other fallback type.
     * @return the narrowest selected type.
     */
    public static RelDataType narrowestTypeFor(Number value, SqlTypeName otherType) {
        if (value instanceof BigDecimal) {
            BigDecimal decimalValue = (BigDecimal) value;
            if (decimalValue.scale() <= 0) {
                try {
                    long longValue = decimalValue.longValueExact();

                    int bitWidth = HazelcastIntegerType.bitWidthOf(longValue);
                    return HazelcastIntegerType.of(bitWidth, false);
                } catch (ArithmeticException e) {
                    return HazelcastTypeFactory.INSTANCE.createSqlType(FRACTIONAL_TYPES.contains(otherType) ? otherType : BIGINT);
                }
            } else {
                return HazelcastTypeFactory.INSTANCE.createSqlType(APPROX_TYPES.contains(otherType) ? otherType : DECIMAL);
            }
        } else {
            assert value instanceof Double;
            return HazelcastTypeFactory.INSTANCE.createSqlType(APPROX_TYPES.contains(otherType) ? otherType : DOUBLE);
        }
    }

    /**
     * @return the SQL type name of the given type.
     */
    public static SqlTypeName typeName(RelDataType type) {
        return type.getSqlTypeName();
    }

    /**
     * @return {@code true} if the given type is a numeric type, {@code false}
     * otherwise.
     * <p>
     * Numeric types are: TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE and
     * DECIMAL.
     */
    public static boolean isNumeric(RelDataType type) {
        return NUMERIC_TYPES.contains(typeName(type));
    }

    /**
     * @return {@code true} if the given type is a char type, {@code false}
     * otherwise.
     * <p>
     * Char types are: CHAR and VARCHAR.
     */
    public static boolean isChar(RelDataType type) {
        return CHAR_TYPES.contains(typeName(type));
    }

    /**
     * @return {@code true} if the given type is a floating-point type, {@code
     * false} otherwise.
     * <p>
     * Floating-point types are: REAL, DOUBLE and DECIMAL.
     */
    public static boolean isFloatingPoint(RelDataType type) {
        return FRACTIONAL_TYPES.contains(typeName(type));
    }

    /**
     * @return {@code true} if the given type is an integer type, {@code false}
     * otherwise.
     * <p>
     * Integer types are: TINYINT, SMALLINT, INTEGER and BIGINT.
     */
    public static boolean isInteger(RelDataType type) {
        return INT_TYPES.contains(type.getSqlTypeName());
    }

    /**
     * @return {@code true} if the given type is a temporal type, {@code false}
     * otherwise.
     * <p>
     * Temporal types are: all variants of DATE, TIME, TIMESTAMP and all variants
     * of interval types.
     */
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
        SqlTypeName typeName = type.getSqlTypeName();

        if (YEAR_INTERVAL_TYPES.contains(typeName)) {
            typeName = INTERVAL_YEAR_MONTH;
        } else if (DAY_INTERVAL_TYPES.contains(typeName)) {
            typeName = INTERVAL_DAY_SECOND;
        }

        QueryDataType hzType = SqlToQueryType.map(typeName);

        return hzType.getTypeFamily().getPrecedence();
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
                // XXX: Calcite returns unscaled value of the internally stored
                // BigDecimal if a long value is requested on the literal.
                BigDecimal decimalValue = literal.getValueAs(BigDecimal.class);
                return decimalValue == null ? null : decimalValue.longValue();

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

            case ANY:
                return literal.getValueAs(Object.class);

            case NULL:
                return null;

            default:
                throw new IllegalArgumentException("unexpected literal type: " + literal.getTypeName());
        }
    }

    private static RelDataType literalType(SqlLiteral literal) {
        return HazelcastTypeFactory.INSTANCE.createSqlType(literal.getTypeName());
    }

}
