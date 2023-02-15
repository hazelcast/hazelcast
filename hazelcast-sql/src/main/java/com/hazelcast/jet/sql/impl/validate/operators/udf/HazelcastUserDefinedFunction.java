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

package com.hazelcast.jet.sql.impl.validate.operators.udf;

import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.operand.TypedOperandChecker;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastFunction;
import com.hazelcast.jet.sql.impl.validate.operators.typeinference.ReplaceUnknownOperandTypeInference;
import com.hazelcast.sql.impl.expression.Expression;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;

import java.lang.reflect.Method;

public final class HazelcastUserDefinedFunction extends HazelcastFunction {

    private final SqlTypeName[] arguments;
    private final Method eval;

    public HazelcastUserDefinedFunction(ScalarUserDefinedFunctionDefinition scalarUdf) {
        super(
                scalarUdf.getName(),
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.explicit(scalarUdf.returnType()),
                new ReplaceUnknownOperandTypeInference(scalarUdf.argumentTypes()),
                SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
        arguments = scalarUdf.argumentTypes();
        eval = scalarUdf.getEvalMethod();
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(arguments.length);
    }

    @Override
    public boolean checkOperandTypes(HazelcastCallBinding callBinding, boolean throwOnFailure) {
        for (int i = 0; i < arguments.length; ++i) {
            // TODO: precreate operand checkers
            if (!TypedOperandChecker.forType(arguments[i]).check(callBinding, throwOnFailure, i)) {
                return false;
            }
        }
        return true;
    }

    public Expression<?> convertCall(Expression<?>[] operands) {
        return new UdfInvocationExpression(eval, operands);
    }

}
