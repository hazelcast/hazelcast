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

package com.hazelcast.sql.impl.calcite.validate;

import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.calcite.literal.HazelcastSqlLiteral;
import com.hazelcast.sql.impl.type.converter.StringConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.util.Locale;

import static org.apache.calcite.sql.type.SqlTypeName.APPROX_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.CHAR_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.EXACT_TYPES;
import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Utility methods to work with {@link SqlNode}s.
 */
public final class SqlNodeUtil {

    private SqlNodeUtil() {
    }

    /**
     * @return {@code true} if the given node is a {@linkplain SqlDynamicParam
     * dynamic parameter}, {@code false} otherwise.
     */
    public static boolean isParameter(SqlNode node) {
        return node.getKind() == SqlKind.DYNAMIC_PARAM;
    }

    /**
     * @return {@code true} if the given node is a {@linkplain SqlLiteral literal},
     * {@code false} otherwise.
     */
    // TODO: Remove candidate
    public static boolean isLiteral_old(SqlNode node) {
        return node.getKind() == SqlKind.LITERAL;
    }

    public static BiTuple<Boolean, Boolean> booleanValue(SqlLiteral literal) {
        switch (literal.getTypeName()) {
            case BOOLEAN:
                return BiTuple.of(true, literal.booleanValue());

            case NULL:
                return BiTuple.of(true, null);

            case CHAR:
            case VARCHAR:
                String value = literal.getValueAs(String.class);

                if (value == null) {
                    return BiTuple.of(true, null);
                } else {
                    value = value.toLowerCase(Locale.ROOT);

                    if (Boolean.TRUE.toString().equals(value)) {
                        return BiTuple.of(true, true);
                    } else if (Boolean.FALSE.toString().equals(value)) {
                        return BiTuple.of(true, false);
                    } else {
                        return BiTuple.of(false, false);
                    }
                }

            default:
                return BiTuple.of(false, false);
        }
    }

    /**
     * Obtains a numeric value of the given node if it's a numeric or string
     * {@linkplain SqlLiteral literal}.
     * <p>
     * If the literal represents an exact value (see {@link
     * SqlTypeName#EXACT_TYPES}), the obtained numeric value is {@link BigDecimal}.
     * Otherwise, if the literal represents an approximate value (see {@link
     * SqlTypeName#APPROX_TYPES}), the obtained numeric value is {@link Double}.
     *
     * @param node the node to obtain the numeric value of.
     * @return the obtained numeric value or {@code null} if the given node is
     * not a numeric or string literal.
     * @throws CalciteContextException if the given node is a string literal
     *                                 that doesn't have a valid numeric
     *                                 representation.
     */
    // TODO: Remove candidate
    public static Number numericValue(SqlNode node) {
        if (node.getKind() != SqlKind.LITERAL) {
            return null;
        }

        SqlLiteral literal = (SqlLiteral) node;
        SqlTypeName typeName = literal.getTypeName();

        if (CHAR_TYPES.contains(typeName)) {
            String value = literal.getValueAs(String.class);
            if (value == null) {
                return null;
            }

            if (value.contains("e") || value.contains("E")) {
                // floating point approximate scientific notation
                try {
                    return StringConverter.INSTANCE.asDouble(value);
                } catch (QueryException e) {
                    assert e.getCode() == SqlErrorCode.DATA_EXCEPTION;
                    throw SqlUtil.newContextException(literal.getParserPosition(),
                            RESOURCE.invalidLiteral(literal.toString(), DOUBLE.getName()));
                }
            } else {
                // floating point exact doted notation or integer
                try {
                    return StringConverter.INSTANCE.asDecimal(value);
                } catch (QueryException e) {
                    assert e.getCode() == SqlErrorCode.DATA_EXCEPTION;
                    throw SqlUtil.newContextException(literal.getParserPosition(),
                            RESOURCE.invalidLiteral(literal.toString(), DECIMAL.getName()));
                }
            }
        } else if (APPROX_TYPES.contains(typeName)) {
            return literal.getValueAs(Double.class);
        } else {
            return EXACT_TYPES.contains(typeName) ? literal.getValueAs(BigDecimal.class) : null;
        }
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

    public static SqlBasicCall asLiteral(SqlNode operand) {
        if (operand instanceof SqlBasicCall) {
            SqlBasicCall call = (SqlBasicCall) operand;

            return call;
        }

        return null;
    }

    public static boolean isLiteral(SqlNode operand) {
        return asLiteral(operand) != null;
    }

    public static boolean isExactNumericLiteral(SqlNode operand) {
        SqlBasicCall call = asLiteral(operand);

        if (call != null) {
            HazelcastSqlLiteral literal = call.operand(0);

            switch (literal.getTypeName()) {
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                    return true;

                default:
                    return false;
            }
        }

        return false;
    }
}
