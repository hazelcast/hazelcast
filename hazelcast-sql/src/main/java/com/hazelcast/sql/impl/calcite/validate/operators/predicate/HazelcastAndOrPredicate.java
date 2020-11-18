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

package com.hazelcast.sql.impl.calcite.validate.operators.predicate;

import com.hazelcast.sql.impl.calcite.validate.binding.SqlCallBindingManualOverride;
import com.hazelcast.sql.impl.calcite.validate.binding.SqlCallBindingOverride;
import com.hazelcast.sql.impl.calcite.validate.operand.BooleanOperandChecker;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.util.Litmus;

public final class HazelcastAndOrPredicate extends SqlBinaryOperator implements SqlCallBindingManualOverride {

    public static final HazelcastAndOrPredicate AND = new HazelcastAndOrPredicate(
        "AND",
        SqlKind.AND,
        SqlStdOperatorTable.AND.getLeftPrec()
    );

    public static final SqlBinaryOperator OR = new HazelcastAndOrPredicate(
        "OR",
        SqlKind.OR,
        SqlStdOperatorTable.OR.getLeftPrec()
    );

    private HazelcastAndOrPredicate(String name, SqlKind kind, int prec) {
        super(
            name,
            kind,
            prec,
            true,
            ReturnTypes.BOOLEAN_NULLABLE,
            InferTypes.BOOLEAN,
            null
        );
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        SqlCallBindingOverride callBinding0 = new SqlCallBindingOverride(callBinding);

        boolean res = true;

        for (int i = 0; i < callBinding.getOperandCount(); i++) {
            boolean operandRes = BooleanOperandChecker.INSTANCE.check(callBinding0, throwOnFailure, i);

            if (!operandRes) {
                res = false;
            }
        }

        return res;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.from(2);
    }

    @Override
    public boolean validRexOperands(int count, Litmus litmus) {
        // Allow for more than two operands similarly to Calcite built-in AND/OR operators.
        // We ooverride the method because Calcite returns "true" only for the instances of the original operators.
        if (count > 2) {
            return true;
        }

        return super.validRexOperands(count, litmus);
    }
}
