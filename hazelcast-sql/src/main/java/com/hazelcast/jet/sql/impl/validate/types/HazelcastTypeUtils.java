/*
 * Copyright 2023 Hazelcast Inc.
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

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.util.StringUtil.equalsIgnoreCase;
import static org.apache.calcite.sql.type.SqlTypeFamily.INTERVAL_DAY_TIME;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_DAY_SECOND;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_YEAR_MONTH;
import static org.apache.calcite.sql.type.SqlTypeName.NULL;
import static org.apache.calcite.sql.type.SqlTypeName.OTHER;
import static org.apache.calcite.sql.type.SqlTypeName.ROW;
import static org.apache.calcite.sql.type.SqlTypeName.UNKNOWN;

/**
 * Provides utilities to map from Calcite's {@link SqlTypeName} to {@link
 * QueryDataType}.
 */
@SuppressWarnings("checkstyle:ExecutableStatementCount")
public final class HazelcastTypeUtils {

    private static final Map<SqlTypeName, QueryDataType> CALCITE_TO_HZ = new HashMap<>();
    private static final Map<QueryDataTypeFamily, SqlTypeName> HZ_TO_CALCITE = new HashMap<>();

    static {
        HZ_TO_CALCITE.put(QueryDataTypeFamily.VARCHAR, SqlTypeName.VARCHAR);
        CALCITE_TO_HZ.put(SqlTypeName.VARCHAR, QueryDataType.VARCHAR);
        CALCITE_TO_HZ.put(SqlTypeName.CHAR, QueryDataType.VARCHAR);

        HZ_TO_CALCITE.put(QueryDataTypeFamily.BOOLEAN, SqlTypeName.BOOLEAN);
        CALCITE_TO_HZ.put(SqlTypeName.BOOLEAN, QueryDataType.BOOLEAN);

        HZ_TO_CALCITE.put(QueryDataTypeFamily.TINYINT, SqlTypeName.TINYINT);
        HZ_TO_CALCITE.put(QueryDataTypeFamily.SMALLINT, SqlTypeName.SMALLINT);
        HZ_TO_CALCITE.put(QueryDataTypeFamily.INTEGER, SqlTypeName.INTEGER);
        HZ_TO_CALCITE.put(QueryDataTypeFamily.BIGINT, SqlTypeName.BIGINT);
        CALCITE_TO_HZ.put(SqlTypeName.TINYINT, QueryDataType.TINYINT);
        CALCITE_TO_HZ.put(SqlTypeName.SMALLINT, QueryDataType.SMALLINT);
        CALCITE_TO_HZ.put(SqlTypeName.INTEGER, QueryDataType.INT);
        CALCITE_TO_HZ.put(SqlTypeName.BIGINT, QueryDataType.BIGINT);

        HZ_TO_CALCITE.put(QueryDataTypeFamily.DECIMAL, SqlTypeName.DECIMAL);
        CALCITE_TO_HZ.put(SqlTypeName.DECIMAL, QueryDataType.DECIMAL);

        HZ_TO_CALCITE.put(QueryDataTypeFamily.REAL, SqlTypeName.REAL);
        HZ_TO_CALCITE.put(QueryDataTypeFamily.DOUBLE, SqlTypeName.DOUBLE);
        CALCITE_TO_HZ.put(SqlTypeName.REAL, QueryDataType.REAL);
        CALCITE_TO_HZ.put(SqlTypeName.DOUBLE, QueryDataType.DOUBLE);

        HZ_TO_CALCITE.put(QueryDataTypeFamily.TIME, SqlTypeName.TIME);
        HZ_TO_CALCITE.put(QueryDataTypeFamily.DATE, SqlTypeName.DATE);
        HZ_TO_CALCITE.put(QueryDataTypeFamily.TIMESTAMP, SqlTypeName.TIMESTAMP);
        HZ_TO_CALCITE.put(QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE, SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        CALCITE_TO_HZ.put(SqlTypeName.TIME, QueryDataType.TIME);
        CALCITE_TO_HZ.put(SqlTypeName.DATE, QueryDataType.DATE);
        CALCITE_TO_HZ.put(SqlTypeName.TIMESTAMP, QueryDataType.TIMESTAMP);
        CALCITE_TO_HZ.put(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME);

        HZ_TO_CALCITE.put(QueryDataTypeFamily.OBJECT, SqlTypeName.ANY);
        CALCITE_TO_HZ.put(SqlTypeName.ANY, QueryDataType.OBJECT);

        HZ_TO_CALCITE.put(QueryDataTypeFamily.NULL, SqlTypeName.NULL);
        CALCITE_TO_HZ.put(SqlTypeName.NULL, QueryDataType.NULL);
        CALCITE_TO_HZ.put(SqlTypeName.UNKNOWN, QueryDataType.NULL);

        // The inverse mapping is not needed, because we map multiple interval type to two internal types.
        HZ_TO_CALCITE.put(QueryDataTypeFamily.INTERVAL_YEAR_MONTH, INTERVAL_YEAR_MONTH);
        HZ_TO_CALCITE.put(QueryDataTypeFamily.INTERVAL_DAY_SECOND, INTERVAL_DAY_SECOND);

        CALCITE_TO_HZ.put(SqlTypeName.MAP, QueryDataType.MAP);
        HZ_TO_CALCITE.put(QueryDataTypeFamily.JSON, OTHER);
        HZ_TO_CALCITE.put(QueryDataTypeFamily.ROW, ROW);
        CALCITE_TO_HZ.put(ROW, QueryDataType.ROW);
    }

    private HazelcastTypeUtils() {
        // No-op.
    }

    public static SqlTypeName toCalciteType(QueryDataType type) {
        return toCalciteType(type.getTypeFamily());
    }

    public static SqlTypeName toCalciteType(QueryDataTypeFamily typeFamily) {
        return HZ_TO_CALCITE.get(typeFamily);
    }

    public static QueryDataType toHazelcastType(RelDataType relDataType) {
        if (relDataType.getSqlTypeName() != OTHER) {
            return toHazelcastTypeFromSqlTypeName(relDataType.getSqlTypeName());
        }
        final RelDataTypeFamily typeFamily = relDataType.getFamily();

        if (typeFamily instanceof HazelcastJsonType) {
            return QueryDataType.JSON;
        }

        if (typeFamily instanceof HazelcastObjectType) {
            return convertHazelcastObjectType(relDataType);
        }

        throw new IllegalArgumentException("Unexpected SQL type: " + relDataType);
    }

    private static QueryDataType convertHazelcastObjectType(final RelDataType relDataType) {
        final HazelcastObjectType hazelcastObjectType = (HazelcastObjectType) relDataType;

        final Map<String, QueryDataType> typeMap = new HashMap<>();
        traverseHzObjectType(hazelcastObjectType, typeMap);

        return typeMap.get(hazelcastObjectType.getTypeName());
    }

    private static void traverseHzObjectType(final HazelcastObjectType source, Map<String, QueryDataType> discovered) {
        if (discovered.containsKey(source.getTypeName())) {
            return;
        }
        final QueryDataType current = new QueryDataType(source.getTypeName());
        discovered.put(current.getObjectTypeName(), current);

        for (final RelDataTypeField field : source.getFieldList()) {
            final QueryDataType fieldType;
            if (field.getType() instanceof HazelcastObjectType) {
                final HazelcastObjectType fieldRelDataType = (HazelcastObjectType) field.getType();
                if (!discovered.containsKey(fieldRelDataType.getTypeName())) {
                    traverseHzObjectType(fieldRelDataType, discovered);
                }
                fieldType = discovered.get(fieldRelDataType.getTypeName());
            } else {
                fieldType = HazelcastTypeUtils.toHazelcastType(field.getType());
            }
            current.getObjectFields().add(field.getIndex(), new QueryDataType.QueryDataTypeField(field.getName(), fieldType));
        }
    }

    public static QueryDataType toHazelcastTypeFromSqlTypeName(SqlTypeName sqlTypeName) {
        SqlTypeFamily sqlTypeNameFamily = sqlTypeName.getFamily();
        if (sqlTypeNameFamily == SqlTypeFamily.INTERVAL_YEAR_MONTH) {
            return QueryDataType.INTERVAL_YEAR_MONTH;
        } else if (sqlTypeNameFamily == SqlTypeFamily.INTERVAL_DAY_TIME) {
            return QueryDataType.INTERVAL_DAY_SECOND;
        }
        QueryDataType queryDataType = CALCITE_TO_HZ.get(sqlTypeName);
        if (queryDataType == null) {
            throw new IllegalArgumentException("Unexpected SQL type: " + sqlTypeName);
        }
        return queryDataType;
    }

    public static RelDataType createType(RelDataTypeFactory typeFactory, SqlTypeName typeName, boolean nullable) {
        RelDataType type = typeFactory.createSqlType(typeName);

        if (nullable) {
            type = createNullableType(typeFactory, type);
        }

        return type;
    }

    public static RelDataType createNullableType(RelDataTypeFactory typeFactory, RelDataType type) {
        if (!type.isNullable()) {
            type = typeFactory.createTypeWithNullability(type, true);
        }

        return type;
    }

    public static boolean isObjectIdentifier(SqlIdentifier identifier) {
        return identifier.isSimple() && equalsIgnoreCase(SqlColumnType.OBJECT.name(), identifier.getSimple());
    }

    public static boolean isJsonIdentifier(SqlIdentifier identifier) {
        return identifier.isSimple() && equalsIgnoreCase(SqlColumnType.JSON.name(), identifier.getSimple());
    }

    public static boolean isJsonType(RelDataType type) {
        return SqlTypeName.OTHER.equals(type.getSqlTypeName()) && HazelcastJsonType.FAMILY.equals(type.getFamily());
    }

    /**
     * @return {@code true} if the given type is a numeric type, {@code false}
     * otherwise.
     * <p>
     * Numeric types are: TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE and
     * DECIMAL.
     */
    public static boolean isNumericType(RelDataType type) {
        return isNumericType(type.getSqlTypeName());
    }

    /**
     * @return {@code true} if the given type is a numeric type, {@code false}
     * otherwise.
     * <p>
     * Numeric types are: TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE and
     * DECIMAL.
     */
    public static boolean isNumericType(SqlTypeName typeName) {
        switch (typeName) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DECIMAL:
            case FLOAT:
            case REAL:
            case DOUBLE:
                return true;

            default:
                return false;
        }
    }

    /**
     * @return {@code true} if the given type is an integer type, {@code false}
     * otherwise.
     * <p>
     * Integer types are: TINYINT, SMALLINT, INTEGER and BIGINT.
     */
    public static boolean isNumericIntegerType(RelDataType type) {
        return isNumericIntegerType(type.getSqlTypeName());
    }

    /**
     * @return {@code true} if the given type is an integer type, {@code false}
     * otherwise.
     * <p>
     * Integer types are: TINYINT, SMALLINT, INTEGER and BIGINT.
     */
    public static boolean isNumericIntegerType(SqlTypeName typeName) {
        switch (typeName) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return true;

            default:
                return false;
        }
    }

    /**
     * @return {@code true} if the given type is an inexact numeric type, {@code false}
     * otherwise.
     * <p>
     * Integer types are: REAL, DOUBLE.
     */
    public static boolean isNumericInexactType(RelDataType type) {
        return isNumericInexactType(type.getSqlTypeName());
    }

    /**
     * @return {@code true} if the given type is an inexact numeric type, {@code false}
     * otherwise.
     * <p>
     * Inexact numeric types are: REAL, DOUBLE.
     */
    public static boolean isNumericInexactType(SqlTypeName typeName) {
        switch (typeName) {
            case REAL:
            case FLOAT:
            case DOUBLE:
                return true;

            default:
                return false;
        }
    }

    public static boolean isTemporalType(RelDataType type) {
        return isTemporalType(type.getSqlTypeName());
    }

    public static boolean isTemporalType(SqlTypeName typeName) {
        switch (typeName) {
            case DATE:
            case TIME:
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return true;

            default:
                return false;
        }
    }

    public static boolean isIntervalType(RelDataType type) {
        return isIntervalType(type.getSqlTypeName());
    }

    public static boolean isIntervalType(SqlTypeName typeName) {
        SqlTypeFamily typeFamily = typeName.getFamily();

        return typeFamily == INTERVAL_DAY_TIME || typeFamily == SqlTypeFamily.INTERVAL_YEAR_MONTH;
    }

    public static boolean isNullOrUnknown(SqlTypeName typeName) {
        return typeName == SqlTypeName.NULL || typeName == SqlTypeName.UNKNOWN;
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
    @SuppressWarnings("checkstyle:BooleanExpressionComplexity")
    public static RelDataType withHigherPrecedence(RelDataType type1, RelDataType type2) {
        int precedence1 = precedenceOf(type1);
        int precedence2 = precedenceOf(type2);
        assert precedence1 != precedence2 || type1.getSqlTypeName() == type2.getSqlTypeName()
                || type1.getSqlTypeName() == NULL && type2.getSqlTypeName() == UNKNOWN
                || type1.getSqlTypeName() == UNKNOWN && type2.getSqlTypeName() == NULL;

        if (precedence1 == precedence2 && isNumericIntegerType(type1) && isNumericIntegerType(type2)) {
            int bitWidth1 = ((HazelcastIntegerType) type1).getBitWidth();
            int bitWidth2 = ((HazelcastIntegerType) type2).getBitWidth();
            return bitWidth1 > bitWidth2 ? type1 : type2;
        }

        return precedence1 > precedence2 ? type1 : type2;
    }

    public static int precedenceOf(RelDataType type) {
        return toHazelcastType(type).getTypeFamily().getPrecedence();
    }

    public static boolean canCast(RelDataType sourceType, RelDataType targetType) {
        if (targetType.equals(sourceType)) {
            return true;
        }

        QueryDataType queryFrom = toHazelcastType(sourceType);
        QueryDataType queryTo = toHazelcastType(targetType);

        if (isStruct(sourceType) || isStruct(targetType)) {
            if (queryFrom.isCustomType() && queryTo.getTypeFamily().equals(QueryDataTypeFamily.JSON)) {
                return true;
            }

            // if one of them isn't a struct
            if (!isStruct(sourceType) || !isStruct(targetType)) {
                return false;
            }

            // ROW source can be converted to target type
            // TODO: target type can be ROW in some expressions?
            if ((sourceType.getSqlTypeName() != targetType.getSqlTypeName())
                    && !(targetType.getSqlTypeName().equals(ROW) || sourceType.getSqlTypeName().equals(ROW))) {
                return false;
            }

            int n = targetType.getFieldCount();
            if (sourceType.getFieldCount() != n) {
                return false;
            }
            for (int i = 0; i < n; ++i) {
                RelDataTypeField toField = targetType.getFieldList().get(i);
                RelDataTypeField fromField = sourceType.getFieldList().get(i);
                if (!canCast(toField.getType(), fromField.getType())) {
                    return false;
                }
            }
            return true;
        }

        return queryFrom.getConverter().canConvertTo(queryTo.getTypeFamily());
    }

    private static boolean isStruct(RelDataType relDataType) {
        return relDataType.isStruct() && relDataType.getFieldCount() > 0;
    }

    public static boolean hasParameters(SqlCallBinding binding) {
        return binding.operands().stream().anyMatch((operand) -> operand.getKind() == SqlKind.DYNAMIC_PARAM);
    }

    public static boolean hasSameTypeFamily(RelDataType sourceType, RelDataType targetType) {
        return sourceType.getFamily().equals(targetType.getFamily());
    }
}
