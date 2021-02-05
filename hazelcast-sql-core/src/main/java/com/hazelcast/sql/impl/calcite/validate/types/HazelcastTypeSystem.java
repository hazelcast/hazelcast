/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;

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

            if (type == BIGINT) {
                // special-case for BIGINT - we use BIGINT(64) instead of the default BIGINT(63) because
                // BIGINT + BIGINT can overflow.
                return HazelcastIntegerType.create(Long.SIZE, argumentType.isNullable());
            }

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
}
