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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.type.SqlTypeName;

public final class LiteralUtils {
    private LiteralUtils() {
        // No-op
    }

    public static Literal literal(SqlNode node) {
        if (node.getKind() != SqlKind.LITERAL) {
            // Not a literal
            return null;
        }

        SqlLiteral literal = (SqlLiteral) node;

        // Do no convert symbols.
        if (literal.getTypeName() == SqlTypeName.SYMBOL) {
            return null;
        }

        if (literal instanceof SqlNumericLiteral) {
            return NumericLiteral.create((SqlNumericLiteral) literal);
        }

        if (literal instanceof SqlCharStringLiteral) {
            return new TypedLiteral(literal, literal.getValueAs(String.class), SqlTypeName.VARCHAR);
        }

        return new TypedLiteral(literal, literal.getValue(), literal.getTypeName());
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
