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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ConversionUtil;

import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.util.List;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.MAX_DECIMAL_PRECISION;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.MAX_DECIMAL_SCALE;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.REAL;

/**
 * Custom Hazelcast type factory.
 * <p>
 * The main purpose of this factory is to plug {@link HazelcastIntegerType} into
 * Calcite runtime.
 */
public final class HazelcastTypeFactory extends SqlTypeFactoryImpl {

    public static final HazelcastTypeFactory INSTANCE = new HazelcastTypeFactory();

    private static final RelDataType TYPE_TIME = new HazelcastType(SqlTypeName.TIME, false);
    private static final RelDataType TYPE_TIME_NULLABLE = new HazelcastType(SqlTypeName.TIME, true);

    private static final RelDataType TYPE_TIMESTAMP = new HazelcastType(SqlTypeName.TIMESTAMP, false);
    private static final RelDataType TYPE_TIMESTAMP_NULLABLE = new HazelcastType(SqlTypeName.TIMESTAMP, true);

    private static final RelDataType TYPE_TIMESTAMP_WITH_TIME_ZONE = new HazelcastType(
        SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, false
    );

    private static final RelDataType TYPE_TIMESTAMP_WITH_TIME_ZONE_NULLABLE = new HazelcastType(
        SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
        true
    );

    private static final RelDataType TYPE_OBJECT = new HazelcastType(SqlTypeName.ANY, false);
    private static final RelDataType TYPE_OBJECT_NULLABLE = new HazelcastType(SqlTypeName.ANY, true);

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
            return super.createSqlType(DECIMAL, MAX_DECIMAL_PRECISION, MAX_DECIMAL_SCALE);
        } else if (typeName == SqlTypeName.ANY) {
            return TYPE_OBJECT;
        } else if (typeName == SqlTypeName.TIME) {
            return TYPE_TIME;
        } else if (typeName == SqlTypeName.TIMESTAMP) {
            return TYPE_TIMESTAMP;
        } else if (typeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            return TYPE_TIMESTAMP_WITH_TIME_ZONE;
        }

        if (HazelcastTypeUtils.isNumericIntegerType(typeName)) {
            return HazelcastIntegerType.create(typeName, false);
        }

        return null;
    }

    @Override
    public RelDataType createTypeWithNullability(RelDataType type, boolean nullable) {
        if (HazelcastTypeUtils.isNumericIntegerType(type.getSqlTypeName())) {
            return HazelcastIntegerType.create((HazelcastIntegerType) type, nullable);
        } else if (type.getSqlTypeName() == SqlTypeName.ANY) {
            return nullable ? TYPE_OBJECT_NULLABLE : TYPE_OBJECT;
        } else if (type.getSqlTypeName() == SqlTypeName.TIME) {
            return nullable ? TYPE_TIME_NULLABLE : TYPE_TIME;
        } else if (type.getSqlTypeName() == SqlTypeName.TIMESTAMP) {
            return nullable ? TYPE_TIMESTAMP_NULLABLE : TYPE_TIMESTAMP;
        } else if (type.getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            return nullable ? TYPE_TIMESTAMP_WITH_TIME_ZONE_NULLABLE : TYPE_TIMESTAMP_WITH_TIME_ZONE;
        }

        return super.createTypeWithNullability(type, nullable);
    }

    @Override
    public RelDataType leastRestrictive(List<RelDataType> types) {
        // Calcite returns BIGINT for all integer types and DOUBLE for all inexact fractional types.
        // This code allows us to use more narrow types in these cases.
        RelDataType selected = super.leastRestrictive(types);

        if (selected == null) {
            return null;
        }

        SqlTypeName selectedTypeName = selected.getSqlTypeName();

        if (HazelcastTypeUtils.isNumericIntegerType(selectedTypeName)) {
            return leastRestrictive(selected, types);
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

    /**
     * Finds the widest bit width integer type belonging to the same type name
     * (family) as the given target integer type from the given list of types.
     *
     * @param targetType the target type to find the widest instance of.
     * @param types      the list of types to inspect.
     * @return the found widest integer type.
     */
    private static RelDataType leastRestrictive(RelDataType targetType, List<RelDataType> types) {
        SqlTypeName typeName = targetType.getSqlTypeName();
        assert HazelcastTypeUtils.isNumericIntegerType(typeName);

        int maxBitWidth = -1;
        RelDataType maxBitWidthType = null;

        for (RelDataType type : types) {
            if (type.getSqlTypeName() != typeName) {
                continue;
            }

            int bitWidth = ((HazelcastIntegerType) type).getBitWidth();

            if (bitWidth > maxBitWidth) {
                maxBitWidth = bitWidth;
                maxBitWidthType = type;
            }
        }
        assert maxBitWidthType != null;
        assert maxBitWidthType.getSqlTypeName() == typeName;

        return HazelcastIntegerType.create((HazelcastIntegerType) maxBitWidthType, targetType.isNullable());
    }
}
