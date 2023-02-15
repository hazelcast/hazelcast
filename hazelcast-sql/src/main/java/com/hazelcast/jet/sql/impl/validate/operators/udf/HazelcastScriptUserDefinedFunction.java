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
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.schema.function.UserDefinedFunction;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;

public final class HazelcastScriptUserDefinedFunction extends HazelcastFunction {

    private final SqlTypeName[] arguments;
    private final QueryDataType returnType;

    public HazelcastScriptUserDefinedFunction(UserDefinedFunction f) {
        super(
                f.getName(),
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.explicit(HazelcastTypeUtils.toCalciteType(f.getReturnType())),
                new ReplaceUnknownOperandTypeInference(
                        f.getParameterTypes().stream().map(HazelcastTypeUtils::toCalciteType).toArray(SqlTypeName[]::new)),
                SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
        arguments = f.getParameterTypes().stream().map(HazelcastTypeUtils::toCalciteType).toArray(SqlTypeName[]::new);
        returnType = f.getReturnType();
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
        return new ScriptUdfInvocationExpression(getName(), returnType, operands);
    }

}
