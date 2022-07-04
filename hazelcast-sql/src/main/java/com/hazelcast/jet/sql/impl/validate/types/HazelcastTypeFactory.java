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

package com.hazelcast.jet.sql.impl.validate.types;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ConversionUtil;

import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.util.List;

import static com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeSystem.MAX_DECIMAL_PRECISION;
import static com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeSystem.MAX_DECIMAL_SCALE;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.REAL;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

/**
 * Custom Hazelcast type factory.
 * <p>
 * The main purpose of this factory is to plug {@link HazelcastIntegerType} into
 * Calcite runtime.
 */
public final class HazelcastTypeFactory extends SqlTypeFactoryImpl {

    public static final HazelcastTypeFactory INSTANCE = new HazelcastTypeFactory();

    private static final RelDataType TYPE_TIME = new HazelcastType(SqlTypeName.TIME, false, 6);
    private static final RelDataType TYPE_TIME_NULLABLE = new HazelcastType(SqlTypeName.TIME, true, 6);

    private static final RelDataType TYPE_TIMESTAMP = new HazelcastType(SqlTypeName.TIMESTAMP, false, 6);
    private static final RelDataType TYPE_TIMESTAMP_NULLABLE = new HazelcastType(SqlTypeName.TIMESTAMP, true, 6);

    private static final RelDataType TYPE_TIMESTAMP_WITH_TIME_ZONE = new HazelcastType(
            SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, false, 6
    );

    private static final RelDataType TYPE_TIMESTAMP_WITH_TIME_ZONE_NULLABLE = new HazelcastType(
            SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
            true,
            6
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

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    @Override
    public RelDataType createTypeWithNullability(RelDataType type, boolean nullable) {
        if (HazelcastTypeUtils.isJsonType(type)) {
            return HazelcastJsonType.create(nullable);
        } else if (HazelcastTypeUtils.isNumericIntegerType(type.getSqlTypeName())) {
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
        // special-case for JSON - see https://github.com/hazelcast/hazelcast/issues/20303
        // SqlTypeName for JSON is OTHER, there's missing handling for OTHER in SqlTypeAssignmentRule,
        // and we don't know how to add it there. And even if we did, OTHER can represent both JSON and
        // a JAVA object, and these aren't assignable.
        boolean containsNullable = false;
        boolean allJson = true;
        boolean allJsonOrVarchar = true;
        for (RelDataType type : types) {
            if (!(type instanceof HazelcastJsonType)) {
                allJson = false;
            }
            if (!(type instanceof HazelcastJsonType) && type.getSqlTypeName() != VARCHAR) {
                allJsonOrVarchar = false;
            }
            if (type.isNullable()) {
                containsNullable = true;
            }
        }
        if (allJson) {
            return containsNullable ? HazelcastJsonType.TYPE_NULLABLE : HazelcastJsonType.TYPE;
        }
        if (allJsonOrVarchar) {
            return createSqlType(VARCHAR, containsNullable);
        }

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
     * @return the widest integer type found.
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
