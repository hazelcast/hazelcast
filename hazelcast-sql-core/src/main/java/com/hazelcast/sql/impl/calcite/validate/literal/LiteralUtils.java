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

package com.hazelcast.sql.impl.calcite.validate.literal;

import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;

import static org.apache.calcite.sql.type.SqlTypeName.CHAR_TYPES;

public final class LiteralUtils {
    private LiteralUtils() {
        // No-op
    }

    public static Literal literal(RexNode node) {
        if (node.getKind() != SqlKind.LITERAL) {
            // Not a literal
            return null;
        }

        RexLiteral literal = (RexLiteral) node;

        return literal0(node.getType().getSqlTypeName(), literal.getValue());
    }

    public static Literal literal(SqlNode node) {
        if (node.getKind() != SqlKind.LITERAL) {
            // Not a literal
            return null;
        }

        SqlLiteral literal = (SqlLiteral) node;
        SqlTypeName typeName = literal.getTypeName();

        Object value = CHAR_TYPES.contains(typeName) ? literal.toValue() : literal.getValue();
        return literal0(typeName, value);
    }

    private static Literal literal0(SqlTypeName typeName, Object value) {
        // Do no convert symbols.
        if (typeName == SqlTypeName.SYMBOL) {
            return null;
        }

        if (HazelcastTypeUtils.isNumericType(typeName)) {
            return NumericLiteral.create(typeName, value);
        }

        if (CHAR_TYPES.contains(typeName)) {
            if (value instanceof NlsString) {
                value = ((NlsString) value).getValue();
            }
            assert value instanceof String : value.getClass().getName();
            return new TypedLiteral(value, SqlTypeName.VARCHAR);
        }

        if (value instanceof SqlIntervalLiteral.IntervalValue) {
            return new IntervalLiteral((SqlIntervalLiteral.IntervalValue) value, typeName);
        }

        return new TypedLiteral(value, typeName);
    }

    public static SqlTypeName literalTypeName(SqlNode node) {
        Literal literal = literal(node);

        return literal != null ? literal.getTypeName() : null;
    }

    public static RelDataType literalType(SqlNode node, HazelcastTypeFactory typeFactory) {
        Literal literal = literal(node);

        return literal != null ? literal.getType(typeFactory) : null;
    }
}
