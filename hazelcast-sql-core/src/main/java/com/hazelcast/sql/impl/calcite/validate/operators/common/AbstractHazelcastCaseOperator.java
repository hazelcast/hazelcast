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

package com.hazelcast.sql.impl.calcite.validate.operators.common;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlCaseOperator;
import org.apache.calcite.sql.type.InferTypes;

import static org.apache.calcite.util.Static.RESOURCE;

public abstract class AbstractHazelcastCaseOperator extends SqlOperator implements HazelcastOperandTypeCheckerAware {

    protected AbstractHazelcastCaseOperator() {
        super(SqlCaseOperator.INSTANCE.getName(), SqlKind.CASE, SqlCaseOperator.INSTANCE.getLeftPrec(), true, null,
                InferTypes.RETURN_TYPE, null);
    }

//    @Override
//    public void validateCall(SqlCall call, SqlValidator validator, SqlValidatorScope scope, SqlValidatorScope operandScope) {
//        SqlCaseOperator.INSTANCE.validateCall(call, validator, scope, operandScope);
//    }

//    @Override
//    public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
//        // SqlCaseOperator is doing the same
//        return validateOperands(validator, scope, call);
//    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding binding) {
        if (binding instanceof RexCallBinding) {
            RexCallBinding rexBinding = (RexCallBinding) binding;
            // strict typing! accepting only if all THEN and ELSE branches have the same types
            // otherwise throw exception
            RelDataType caseType = rexBinding.getOperandType(1);
            int size = rexBinding.getOperandCount();
            for (int i = 3; i < size; i += 2) {
                if (!caseType.equals(rexBinding.getOperandType(i))) {
                    throw QueryException.error("Cannot infer return type of case operator");
                }
            }
            return caseType;
        } else if (binding instanceof SqlCallBinding) {
            SqlCallBinding sqlBinding = (SqlCallBinding) binding;
            SqlCase call = (SqlCase) sqlBinding.getCall();
            HazelcastSqlValidator validator = (HazelcastSqlValidator) sqlBinding.getValidator();

            RelDataType caseType = validator.deriveType(sqlBinding.getScope(), call);
            if (caseType == null) {
                throw sqlBinding.newValidationError(RESOURCE.dynamicParamIllegal());
            }

            for (SqlNode thenOperand : call.getThenOperands()) {
                RelDataType thenOperandType = validator.deriveType(sqlBinding.getScope(), thenOperand);
                if (!caseType.getSqlTypeName().equals(thenOperandType.getSqlTypeName())) {
                    throw sqlBinding.newValidationError(RESOURCE.illegalMixingOfTypes());
                }
            }
            SqlNode elseOperand = call.getElseOperand();
            RelDataType elseOperandType = validator.deriveType(sqlBinding.getScope(), elseOperand);
            if (!caseType.getSqlTypeName().equals(elseOperandType.getSqlTypeName())) {
                throw sqlBinding.newValidationError(RESOURCE.illegalMixingOfTypes());
            }

            return caseType;
        } else {
            return SqlCaseOperator.INSTANCE.inferReturnType(binding);
        }
    }

    public final boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        HazelcastCallBinding bindingOverride = prepareBinding(callBinding);

        return checkOperandTypes(bindingOverride, throwOnFailure);
    }

    @Override
    public SqlSyntax getSyntax() {
        return SqlCaseOperator.INSTANCE.getSyntax();
    }

    protected abstract boolean checkOperandTypes(HazelcastCallBinding callBinding, boolean throwOnFailure);

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        writer.print("CASE");
    }
}
