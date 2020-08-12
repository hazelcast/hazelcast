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

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlOperator;
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

    private static void rewriteCall(SqlCall call) {
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

}
