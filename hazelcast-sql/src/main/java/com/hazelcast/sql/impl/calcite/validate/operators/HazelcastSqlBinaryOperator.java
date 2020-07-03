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
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.util.Litmus;

/**
 * The same as {@link SqlBinaryOperator}, but supports monotonicity for NULL
 * literals and operators from our custom {@link HazelcastSqlOperatorTable}.
 */
public class HazelcastSqlBinaryOperator extends SqlBinaryOperator {

    public HazelcastSqlBinaryOperator(String name, SqlKind kind, int prec, boolean leftAssoc,
                                      SqlReturnTypeInference returnTypeInference, SqlOperandTypeInference operandTypeInference,
                                      SqlOperandTypeChecker operandTypeChecker) {
        super(name, kind, prec, leftAssoc, returnTypeInference, operandTypeInference, operandTypeChecker);
    }

    @Override
    public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
        if (call.isOperandNull(0, true) || call.isOperandNull(1, true)) {
            return SqlMonotonicity.CONSTANT;
        }

        return super.getMonotonicity(call);
    }

    @Override
    public boolean validRexOperands(int count, Litmus litmus) {
        // XXX: super method does the same, but works only for operators from
        // Calcite's standard SqlStdOperatorTable.
        if ((this == HazelcastSqlOperatorTable.AND || this == HazelcastSqlOperatorTable.OR) && count > 2) {
            return true;
        }
        return super.validRexOperands(count, litmus);
    }

}
