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

package com.hazelcast.jet.sql.impl.parse;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nullable;

/*
 * EXPLAIN PLAN FOR query.
 */
public class SqlExplainStatement extends SqlExplain {
    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("EXPLAIN", SqlKind.EXPLAIN) {
                @SuppressWarnings("argument.type.incompatible")
                @Override
                public SqlCall createCall(@Nullable SqlLiteral functionQualifier,
                                          SqlParserPos pos, @Nullable SqlNode... operands) {
                    return new SqlExplainStatement(pos, operands[0]);
                }
            };

    public SqlExplainStatement(
            SqlParserPos pos,
            SqlNode explicandum) {
        super(pos,
                explicandum,
                SqlExplainLevel.EXPPLAN_ATTRIBUTES.symbol(pos),
                Depth.PHYSICAL.symbol(pos),
                SqlExplainFormat.TEXT.symbol(pos),
                0
        );
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }
}
