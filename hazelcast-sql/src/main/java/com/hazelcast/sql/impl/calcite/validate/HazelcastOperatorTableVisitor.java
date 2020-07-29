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

import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastSqlCase;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;

import java.util.ArrayList;
import java.util.List;

/**
 * Rewrites operators in SqlNode tree from Calcite ones to Hazelcast ones.
 * <p>
 * {@link HazelcastSqlOperatorTable} provides operators with customized type
 * inference and validation, while Calcite parser always uses {@link
 * SqlStdOperatorTable} to resolve operators. This visitor workarounds that by
 * rewriting the standard Calcite operator implementations with the customized
 * ones.
 *
 * @see SqlStdOperatorTable
 * @see HazelcastSqlOperatorTable
 */
public final class HazelcastOperatorTableVisitor extends SqlBasicVisitor<Void> {

    /**
     * Shared Hazelcast operator visitor instance.
     */
    public static final SqlBasicVisitor<Void> INSTANCE = new HazelcastOperatorTableVisitor();

    private static final SqlNameMatcher NAME_MATCHER = SqlNameMatchers.withCaseSensitive(false);

    private HazelcastOperatorTableVisitor() {
    }

    @Override
    public Void visit(SqlCall call) {
        rewriteCall(call);
        return super.visit(call);
    }

    @Override
    public Void visit(SqlNodeList nodeList) {
        rewriteNodeList(nodeList);
        return super.visit(nodeList);
    }

    private static void rewriteCall(SqlCall call) {
        List<SqlNode> operands = call.getOperandList();
        for (int i = 0; i < operands.size(); ++i) {
            SqlNode operand = operands.get(i);

            SqlNode rewrittenCase = tryRewriteCase(operand);
            if (rewrittenCase != null) {
                call.setOperand(i, rewrittenCase);
            }
        }

        if (call instanceof SqlBasicCall) {
            SqlBasicCall basicCall = (SqlBasicCall) call;
            SqlOperator operator = basicCall.getOperator();

            List<SqlOperator> resolvedOperators = new ArrayList<>();
            HazelcastSqlOperatorTable.instance().lookupOperatorOverloads(operator.getNameAsId(), null, operator.getSyntax(),
                    resolvedOperators, NAME_MATCHER);

            if (!resolvedOperators.isEmpty()) {
                assert resolvedOperators.size() == 1;
                basicCall.setOperator(resolvedOperators.get(0));
            }
        }
    }

    private static void rewriteNodeList(SqlNodeList nodeList) {
        for (int i = 0; i < nodeList.size(); ++i) {
            SqlNode node = nodeList.get(i);

            SqlNode rewrittenCase = tryRewriteCase(node);
            if (rewrittenCase != null) {
                nodeList.set(i, rewrittenCase);
            }
        }
    }

    private static SqlNode tryRewriteCase(SqlNode node) {
        if (node instanceof SqlCase && !(node instanceof HazelcastSqlCase)) {
            SqlCase sqlCase = (SqlCase) node;
            return new HazelcastSqlCase(sqlCase.getParserPosition(), sqlCase.getValueOperand(), sqlCase.getWhenOperands(),
                    sqlCase.getThenOperands(), sqlCase.getElseOperand());
        }
        return null;
    }

}
