/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.validate.operators.misc;

import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.HazelcastResources;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.calcite.validate.operators.CoalesceOperandTypeInference;
import com.hazelcast.sql.impl.calcite.validate.operators.common.HazelcastFunction;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.CalciteContextException;
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
import java.util.function.Supplier;

import static com.hazelcast.sql.impl.calcite.validate.operators.HazelcastReturnTypeInference.wrap;
import static com.hazelcast.sql.impl.calcite.validate.operators.misc.HazelcastCaseOperator.coerceItem;

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

        //noinspection OptionalGetWithoutIsPresent
        RelDataType returnType = argTypes.stream().reduce(HazelcastTypeUtils::withHigherPrecedence).get();

        Supplier<CalciteContextException> exceptionSupplier = () ->
                validator.newValidationError(
                        sqlCall, HazelcastResources.RESOURCES.cannotInferCaseResult(argTypes.toString(), "COALESCE"));

        for (int i = 0; i < operandList.size(); i++) {
            int finalI = i;
            if (!coerceItem(
                    validator,
                    scope,
                    operandList.get(i),
                    returnType,
                    sqlNode -> sqlCall.getOperands()[finalI] = sqlNode,
                    throwOnFailure,
                    exceptionSupplier)
            ) {
                return false;
            }
        }

        return true;
    }
}
