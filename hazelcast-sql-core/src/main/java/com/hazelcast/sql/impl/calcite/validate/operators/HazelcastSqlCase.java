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

package com.hazelcast.sql.impl.calcite.validate.operators;

import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Custom Hazelcast {@link SqlCase} node to report {@link HazelcastCaseOperator
 * custom Hazelcast CASE operator} as its operator to override the default
 * return type inference strategy.
 *
 * @see HazelcastCaseOperator#inferReturnType(org.apache.calcite.sql.SqlOperatorBinding)
 */
public final class HazelcastSqlCase extends SqlCase {

    public HazelcastSqlCase(SqlParserPos pos, SqlNode value, SqlNodeList whenList, SqlNodeList thenList, SqlNode elseExpr) {
        super(pos, value, whenList, thenList, elseExpr);
    }

    @Override
    @Nonnull
    public SqlOperator getOperator() {
        return HazelcastSqlOperatorTable.CASE;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.print("CASE\n");
        for (int i = 0; i < getWhenOperands().size(); i++) {
            writer.print("WHEN " + getWhenOperands().get(i) + " THEN " + getThenOperands().get(i) + "\n");
        }
        writer.print("ELSE " + getElseOperand() + "\n");
    }

    @Override
    @Nonnull
    public List<SqlNode> getOperandList() {
        return super.getOperandList().stream().filter(Objects::nonNull).collect(Collectors.toList());
    }
}
