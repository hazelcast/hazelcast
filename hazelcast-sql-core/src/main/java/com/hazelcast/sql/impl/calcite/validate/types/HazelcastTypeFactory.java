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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ConversionUtil;

import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.util.List;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.MAX_DECIMAL_PRECISION;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.MAX_DECIMAL_SCALE;
import static org.apache.calcite.sql.type.SqlTypeName.ANY;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.REAL;
import static org.apache.calcite.sql.type.SqlTypeName.TIME;
import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP;
import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;

/**
 * Custom Hazelcast type factory.
 * <p>
 * The main purpose of this factory is to plug {@link HazelcastIntegerType} into
 * Calcite runtime.
 */
public final class HazelcastTypeFactory extends SqlTypeFactoryImpl {

    /**
     * Shared Hazelcast type factory instance.
     */
    public static final HazelcastTypeFactory INSTANCE = new HazelcastTypeFactory();

    private HazelcastTypeFactory() {
        super(HazelcastTypeSystem.INSTANCE);
    }

    /**
     * Creates a new type of the given type name and nullability.
     * <p>
     * Combines the functionality of {@link #createSqlType(SqlTypeName)} and
     * {@link #createTypeWithNullability(RelDataType, boolean)} into a single
     * call.
     *
     * @param typeName the type of the new type.
     * @param nullable the nullability of the new type.
     * @return the new type created.
     */
    public RelDataType createSqlType(SqlTypeName typeName, boolean nullable) {
        RelDataType type = createSqlType(typeName);
        assert !type.isNullable();

        if (nullable) {
            type = createTypeWithNullability(type, true);
        }

        return type;
    }

    @Override
    public Charset getDefaultCharset() {
        // Calcite uses Latin-1 by default (see {@code CalciteSystemProperty.DEFAULT_CHARSET}). We use unicode.
        return Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME);
    }

    @Override
    public RelDataType createSqlType(SqlTypeName typeName) {
        RelDataType type = createType(typeName);

        if (type == null) {
            type = super.createSqlType(typeName);
        }

        return type;
    }

    @Override
    public RelDataType createSqlType(SqlTypeName typeName, int precision) {
        RelDataType type = createType(typeName);

        if (type == null) {
            type = super.createSqlType(typeName, precision);
        }

        return type;
    }

    @Override
    public RelDataType createSqlType(SqlTypeName typeName, int precision, int scale) {
        RelDataType type = createType(typeName);

        if (type == null) {
            type = super.createSqlType(typeName, precision, scale);
        }

        return type;
    }

    @Nullable
    private RelDataType createType(SqlTypeName typeName) {
        if (typeName == DECIMAL) {
            return createDecimal();
        } else if (typeName == ANY) {
            return HazelcastObjectType.INSTANCE;
        } else if (typeName == TIME) {
            return HazelcastTemporalType.TIME;
        } else if (typeName == TIMESTAMP) {
            return HazelcastTemporalType.TIMESTAMP;
        } else if (typeName == TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            return HazelcastTemporalType.TIMESTAMP_WITH_TIME_ZONE;
        }

        if (HazelcastIntegerType.supports(typeName)) {
            return HazelcastIntegerType.of(typeName);
        }

        return null;
    }

    @Override
    public RelDataType createTypeWithNullability(RelDataType type, boolean nullable) {
        if (HazelcastIntegerType.supports(type.getSqlTypeName())) {
            return HazelcastIntegerType.of(type, nullable);
        } else if (type.getSqlTypeName() == ANY) {
            return nullable ? HazelcastObjectType.NULLABLE_INSTANCE : HazelcastObjectType.INSTANCE;
        } else if (type.getSqlTypeName() == TIME) {
            return nullable ? HazelcastTemporalType.TIME_NULLABLE : HazelcastTemporalType.TIME;
        } else if (type.getSqlTypeName() == TIMESTAMP) {
            return nullable ? HazelcastTemporalType.TIMESTAMP_NULLABLE : HazelcastTemporalType.TIMESTAMP;
        } else if (type.getSqlTypeName() == TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            return nullable
                ? HazelcastTemporalType.TIMESTAMP_WITH_TIME_ZONE_NULLABLE : HazelcastTemporalType.TIMESTAMP_WITH_TIME_ZONE;
        }

        return super.createTypeWithNullability(type, nullable);
    }

    @Override
    public RelDataType leastRestrictive(List<RelDataType> types) {
        // XXX: Calcite infers imprecise types: BIGINT for any integer type and
        // DOUBLE for any floating point type (except DECIMAL). The code bellow
        // fixes that.

        RelDataType selected = super.leastRestrictive(types);
        if (selected == null) {
            return null;
        }
        SqlTypeName selectedTypeName = selected.getSqlTypeName();

        if (HazelcastIntegerType.supports(selectedTypeName)) {
            return HazelcastIntegerType.leastRestrictive(selected, types);
        }

        if (selectedTypeName == DOUBLE) {
            boolean seenDouble = false;
            boolean seenReal = false;

            for (RelDataType type : types) {
                if (type.getSqlTypeName() == DOUBLE) {
                    seenDouble = true;
                    break;
                }
                if (type.getSqlTypeName() == REAL) {
                    seenReal = true;
                }
            }

            if (!seenDouble && seenReal) {
                selected = createSqlType(REAL, selected.isNullable());
            }
        }

        return selected;
    }

    private RelDataType createDecimal() {
        // Produces a strange type: DECIMAL(38, 38), but since we are not tracking
        // precision and scale for DECIMALs, that's fine for our purposes.
        return super.createSqlType(DECIMAL, MAX_DECIMAL_PRECISION, MAX_DECIMAL_SCALE);
    }

}
