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

package com.hazelcast.jet.sql.impl.validate.operators.special;

import com.hazelcast.jet.sql.impl.parse.SqlAnalyzeStatement;
import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastSpecialOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nullable;

public class HazelcastAnalyzeOperator extends HazelcastSpecialOperator {
    public HazelcastAnalyzeOperator() {
        super("ANALYZE", SqlKind.OTHER);
    }
    @Override
    protected boolean checkOperandTypes(final HazelcastCallBinding callBinding, final boolean throwOnFailure) {
        throw new UnsupportedOperationException("Never called for ANALYZE");
    }

    @Override
    public SqlCall createCall(@Nullable SqlLiteral functionQualifier, SqlParserPos pos, @Nullable SqlNode... operands) {
        assert operands != null;
        return new SqlAnalyzeStatement(pos, operands[0]);
    }

    @Override
    public void unparse(final SqlWriter writer, final SqlCall call, final int leftPrec, final int rightPrec) {
        writer.keyword("ANALYZE");
        SqlAnalyzeStatement explainStatement = (SqlAnalyzeStatement) call;
        explainStatement.getQuery().unparse(writer, leftPrec, rightPrec);
    }
}
