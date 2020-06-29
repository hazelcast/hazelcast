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

import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.converter.StringConverter;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUtil;

import java.math.BigDecimal;

import static org.apache.calcite.sql.type.SqlTypeName.CHAR_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.NUMERIC_TYPES;
import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Utility methods to work with {@link SqlNode}s.
 */
public final class SqlNodeUtil {

    private SqlNodeUtil() {
    }

    /**
     * @return {@code true} if the given node is a {@link SqlDynamicParam
     * dynamic parameter}, {@code false} otherwise.
     */
    public static boolean isParameter(SqlNode node) {
        return node.getKind() == SqlKind.DYNAMIC_PARAM;
    }

    /**
     * @return {@code true} if the given node is a {@link SqlLiteral literal},
     * {@code false} otherwise.
     */
    public static boolean isLiteral(SqlNode node) {
        return node.getKind() == SqlKind.LITERAL;
    }

    /**
     * Obtains a numeric value of the given node if it's a numeric or string
     * {@link SqlLiteral literal}.
     * <p>
     *
     * @param node the node to obtain the numeric value of.
     * @return the obtained numeric value or {@code null} if the given node is
     * not a numeric or string literal.
     * @throws CalciteContextException if the given node is a string literal
     *                                 that doesn't have a valid numeric
     *                                 representation.
     */
    public static BigDecimal numericValue(SqlNode node) {
        if (node.getKind() != SqlKind.LITERAL) {
            return null;
        }

        SqlLiteral literal = (SqlLiteral) node;

        if (CHAR_TYPES.contains(literal.getTypeName())) {
            try {
                return StringConverter.INSTANCE.asDecimal(literal.getValueAs(String.class));
            } catch (QueryException e) {
                assert e.getCode() == SqlErrorCode.DATA_EXCEPTION;
                throw SqlUtil.newContextException(literal.getParserPosition(),
                        RESOURCE.invalidLiteral(literal.toString(), DECIMAL.getName()));
            }
        } else {
            return NUMERIC_TYPES.contains(literal.getTypeName()) ? literal.getValueAs(BigDecimal.class) : null;
        }
    }

}
