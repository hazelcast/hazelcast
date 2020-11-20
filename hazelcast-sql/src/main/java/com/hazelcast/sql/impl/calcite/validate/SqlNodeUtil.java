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

import com.hazelcast.sql.impl.calcite.literal.HazelcastSqlLiteral;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Utility methods to work with {@link SqlNode}s.
 */
public final class SqlNodeUtil {
    private SqlNodeUtil() {
        // No-op.
    }

    /**
     * @return {@code true} if the given node is a {@linkplain SqlDynamicParam
     * dynamic parameter}, {@code false} otherwise.
     */
    public static boolean isParameter(SqlNode node) {
        return node.getKind() == SqlKind.DYNAMIC_PARAM;
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

    public static SqlTypeName literalTypeName(SqlNode operand) {
        if (operand instanceof SqlLiteral) {
            HazelcastSqlLiteral literal = HazelcastSqlLiteral.convert((SqlLiteral) operand);

            if (literal != null) {
                return literal.getTypeName();
            }
        }

        return null;
    }

    public static RelDataType literalType(SqlNode operand, HazelcastTypeFactory typeFactory) {
        if (operand instanceof SqlLiteral) {
            HazelcastSqlLiteral literal = HazelcastSqlLiteral.convert((SqlLiteral) operand);

            if (literal != null) {
                return literal.getType(typeFactory);
            }
        }

        return null;
    }

    public static boolean isExactNumericLiteral(SqlNode operand) {
        SqlTypeName type = literalTypeName(operand);

        if (type != null) {
            switch (type) {
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
