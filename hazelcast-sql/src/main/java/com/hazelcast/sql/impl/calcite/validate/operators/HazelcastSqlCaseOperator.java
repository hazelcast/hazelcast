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

import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlCaseOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Custom Hazelcast {@link SqlCaseOperator} to override the default return type
 * inference strategy for CASE.
 *
 * @see HazelcastSqlCase
 */
public final class HazelcastSqlCaseOperator extends SqlOperator {

    public HazelcastSqlCaseOperator() {
        super(SqlCaseOperator.INSTANCE.getName(), SqlKind.CASE, MDX_PRECEDENCE, true, null, InferTypes.RETURN_TYPE, null);
    }

    @Override
    public void validateCall(SqlCall call, SqlValidator validator, SqlValidatorScope scope, SqlValidatorScope operandScope) {
        SqlCaseOperator.INSTANCE.validateCall(call, validator, scope, operandScope);
    }

    @Override
    public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
        // SqlCaseOperator is doing the same
        return validateOperands(validator, scope, call);
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding binding, boolean throwOnFailure) {
        return SqlCaseOperator.INSTANCE.checkOperandTypes(binding, throwOnFailure);
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding binding) {
        if (binding instanceof SqlCallBinding) {
            SqlCallBinding sqlBinding = (SqlCallBinding) binding;
            SqlCase call = (SqlCase) sqlBinding.getCall();
            HazelcastSqlValidator validator = (HazelcastSqlValidator) sqlBinding.getValidator();

            validator.getTypeCoercion().caseWhenCoercion(sqlBinding);
            RelDataType caseType = validator.getKnownNodeType(call);
            if (caseType == null) {
                throw sqlBinding.newValidationError(RESOURCE.dynamicParamIllegal());
            }

            for (SqlNode thenOperand : call.getThenOperands()) {
                RelDataType thenOperandType = validator.deriveType(sqlBinding.getScope(), thenOperand);
                if (!HazelcastTypeSystem.canCast(thenOperandType, caseType)) {
                    throw sqlBinding.newValidationError(RESOURCE.illegalMixingOfTypes());
                }
            }
            SqlNode elseOperand = call.getElseOperand();
            RelDataType elseOperandType = validator.deriveType(sqlBinding.getScope(), elseOperand);
            if (!HazelcastTypeSystem.canCast(elseOperandType, caseType)) {
                throw sqlBinding.newValidationError(RESOURCE.illegalMixingOfTypes());
            }

            return caseType;
        } else {
            return SqlCaseOperator.INSTANCE.inferReturnType(binding);
        }
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlCaseOperator.INSTANCE.getOperandCountRange();
    }

    @Override
    public SqlSyntax getSyntax() {
        return SqlCaseOperator.INSTANCE.getSyntax();
    }

    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
        return new HazelcastSqlCase(pos, operands[0], (SqlNodeList) operands[1], (SqlNodeList) operands[2], operands[3]);
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        SqlCaseOperator.INSTANCE.unparse(writer, call, leftPrec, rightPrec);
    }

}
