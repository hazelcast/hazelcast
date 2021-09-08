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

package com.hazelcast.jet.sql.impl.validate.operators.predicate;

import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.operand.OperandChecker;
import com.hazelcast.jet.sql.impl.validate.operand.OperandCheckerProgram;
import com.hazelcast.jet.sql.impl.validate.operand.TypedOperandChecker;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastBinaryOperator;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.util.Litmus;

import java.util.Arrays;

public final class HazelcastAndOrPredicate extends HazelcastBinaryOperator {

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
                InferTypes.BOOLEAN
        );
    }

    @Override
    public boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
        OperandChecker[] checkers = new OperandChecker[binding.getOperandCount()];
        Arrays.fill(checkers, TypedOperandChecker.BOOLEAN);

        return new OperandCheckerProgram(checkers).check(binding, throwOnFailure);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.from(2);
    }

    @Override
    public boolean validRexOperands(int count, Litmus litmus) {
        // Allow for more than two operands similarly to Calcite built-in AND/OR operators.
        // We override the method because Calcite returns "true" only for the instances of the original operators.
        if (count > 2) {
            return true;
        }

        return super.validRexOperands(count, litmus);
    }
}
