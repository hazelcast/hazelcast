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

package com.hazelcast.jet.sql.impl.validate.operators.misc;

import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.HazelcastSqlValidator;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastFunction;
import com.hazelcast.jet.sql.impl.validate.operators.typeinference.CoalesceOperandTypeInference;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.sql.impl.validate.HazelcastResources.RESOURCES;
import static com.hazelcast.jet.sql.impl.validate.operators.typeinference.HazelcastReturnTypeInference.wrap;

public final class HazelcastCoalesceFunction extends HazelcastFunction {
    public static final HazelcastCoalesceFunction INSTANCE = new HazelcastCoalesceFunction();

    private HazelcastCoalesceFunction() {
        super(
                "COALESCE",
                SqlKind.COALESCE,
                wrap(ReturnTypes.ARG0_NULLABLE),
                CoalesceOperandTypeInference.INSTANCE,
                SqlFunctionCategory.SYSTEM);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.from(1);
    }

    @Override
    protected boolean checkOperandTypes(HazelcastCallBinding callBinding, boolean throwOnFailure) {
        HazelcastSqlValidator validator = callBinding.getValidator();
        SqlValidatorScope scope = callBinding.getScope();

        SqlBasicCall sqlCall = (SqlBasicCall) callBinding.getCall();
        List<SqlNode> operandList = sqlCall.getOperandList();

        List<RelDataType> argTypes = new ArrayList<>(operandList.size());
        for (SqlNode node : operandList) {
            argTypes.add(validator.deriveType(scope, node));
        }

        assert !argTypes.isEmpty();
        RelDataType returnType = argTypes.stream().reduce(HazelcastTypeUtils::withHigherPrecedence).get();

        for (int i = 0; i < operandList.size(); i++) {
            int finalI = i;
            boolean elementTypeCoerced = validator.getTypeCoercion().rowTypeElementCoercion(
                    scope,
                    operandList.get(i),
                    returnType,
                    sqlNode -> sqlCall.setOperand(finalI, sqlNode));

            if (!elementTypeCoerced) {
                if (throwOnFailure) {
                    throw validator.newValidationError(sqlCall, RESOURCES.cannotInferCaseResult(argTypes.toString(), getName()));
                } else {
                    return false;
                }
            }
        }

        return true;
    }
}
