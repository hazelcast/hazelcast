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

import com.hazelcast.sql.impl.calcite.SqlToQueryType;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.calcite.validate.HazelcastTypeCoercion;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

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
// TODO: Move statics out of here
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

    private HazelcastTypeSystem() {
        // No-op
    }

    @Override
    public int getMaxNumericPrecision() {
        return MAX_DECIMAL_PRECISION;
    }

    @Override
    public int getMaxNumericScale() {
        return MAX_DECIMAL_SCALE;
    }

    @Override
    public RelDataType deriveSumType(RelDataTypeFactory typeFactory, RelDataType argumentType) {
        if (argumentType instanceof BasicSqlType) {
            SqlTypeName type = deriveSumType(argumentType.getSqlTypeName());

            if (type.allowsPrec() && argumentType.getPrecision() != RelDataType.PRECISION_NOT_SPECIFIED) {
                int precision = typeFactory.getTypeSystem().getMaxPrecision(type);
                if (type.allowsScale()) {
                    return typeFactory.createTypeWithNullability(
                            typeFactory.createSqlType(type, precision, argumentType.getScale()),
                            argumentType.isNullable()
                    );
                } else {
                    return typeFactory.createTypeWithNullability(
                            typeFactory.createSqlType(type, precision),
                            argumentType.isNullable()
                    );
                }
            } else {
                return typeFactory.createTypeWithNullability(
                        typeFactory.createSqlType(type),
                        argumentType.isNullable()
                );
            }
        }
        return argumentType;
    }

    private static SqlTypeName deriveSumType(SqlTypeName type) {
        switch (type) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return SqlTypeName.BIGINT;
            case DECIMAL:
                return SqlTypeName.DECIMAL;
            case REAL:
            case DOUBLE:
                return SqlTypeName.DOUBLE;
            default:
                return type;
        }
    }

    @Override
    public RelDataType deriveAvgAggType(RelDataTypeFactory typeFactory, RelDataType argumentType) {
        switch (argumentType.getSqlTypeName()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DECIMAL:
                return typeFactory.createTypeWithNullability(
                        typeFactory.createSqlType(DECIMAL),
                        argumentType.isNullable()
                );
            case REAL:
            case DOUBLE:
                return typeFactory.createTypeWithNullability(
                        typeFactory.createSqlType(DOUBLE),
                        argumentType.isNullable()
                );
            default:
                return argumentType;
        }
    }


    /**
     * @return the SQL type name of the given type.
     */
    public static SqlTypeName typeName(RelDataType type) {
        return type.getSqlTypeName();
    }

    /**
     * @return {@code true} if the given identifier specifies OBJECT type,
     * {@code false} otherwise.
     */
    public static boolean isObject(SqlIdentifier identifier) {
        return identifier.isSimple() && QueryDataTypeFamily.OBJECT.name().equalsIgnoreCase(identifier.getSimple());
    }

    public static boolean isTimestampWithTimeZone(SqlIdentifier identifier) {
        return identifier.isSimple()
            && QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE.name().equalsIgnoreCase(identifier.getSimple());
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
}
