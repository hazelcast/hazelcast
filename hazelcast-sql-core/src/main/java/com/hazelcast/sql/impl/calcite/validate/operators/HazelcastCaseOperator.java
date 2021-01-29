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

import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlCase;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlBasicCall;
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
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlCaseOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.implicit.TypeCoercion;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.sql.impl.calcite.validate.operators.HazelcastReturnTypeInference.wrap;
import static org.apache.calcite.util.Static.RESOURCE;

public final class HazelcastCaseOperator extends SqlOperator {

    public static final HazelcastCaseOperator INSTANCE = new HazelcastCaseOperator();

    private HazelcastCaseOperator() {
        super(SqlCaseOperator.INSTANCE.getName(), SqlKind.CASE, SqlCaseOperator.INSTANCE.getLeftPrec(), true,
                wrap(new CaseReturnTypeInference()), null, null);
    }

    @Override
    public void validateCall(SqlCall call, SqlValidator validator, SqlValidatorScope scope, SqlValidatorScope operandScope) {
        System.out.println("HazelcastCaseOperator#validateCall");

        final SqlCase sqlCase = (SqlCase) call;
        final SqlNodeList whenOperands = sqlCase.getWhenOperands();
        final SqlNodeList thenOperands = sqlCase.getThenOperands();
        final SqlNode elseOperand = sqlCase.getElseOperand();
        for (SqlNode operand : whenOperands) {
            operand.validateExpr(validator, operandScope);
        }
        for (SqlNode operand : thenOperands) {
            operand.validateExpr(validator, operandScope);
        }
        if (elseOperand != null) {
            elseOperand.validateExpr(validator, operandScope);
        }
    }

    @Override
    public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
        // SqlCaseOperator is doing the same
        System.out.println("HazelcastCaseOperator#deriveType");
        return validateOperands(validator, scope, call);
    }

    @Override
    // override this methods because passing null into constructor for SqlOperandTypeChecker
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.any();
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        System.out.println("HazelcastCaseOperator#checkOperandTypes");

        SqlNodeList whenList = (SqlNodeList) callBinding.getCall().getOperandList().get(1);
        SqlNodeList thenList = (SqlNodeList) callBinding.getCall().getOperandList().get(2);
        SqlNode elseOperand = callBinding.getCall().getOperandList().get(3);
        assert whenList.size() == thenList.size();

        // checking that search conditions are ok...
        for (SqlNode node : whenList) {
            // should throw validation error if something wrong...
            RelDataType type = callBinding.getValidator().deriveType(callBinding.getScope(), node);
            if (type.getSqlTypeName() != SqlTypeName.BOOLEAN) {
                if (throwOnFailure) {
                    throw callBinding.newError(RESOURCE.expectedBoolean());
                }
                return false;
            }
        }

        boolean foundNotNull = false;
        for (SqlNode node : thenList) {
            RelDataType type = callBinding.getValidator().deriveType(callBinding.getScope(), node);
            if (type.getSqlTypeName() == SqlTypeName.NULL) {
                foundNotNull = true;
            }
        }

        RelDataType elseType = callBinding.getValidator().deriveType(callBinding.getScope(), elseOperand);
        if (elseType.getSqlTypeName() == SqlTypeName.NULL) {
            foundNotNull = true;
        }

        if (!foundNotNull) {
            // according to the sql standard we can not have all of the THEN
            // statements and the ELSE returning null
            if (throwOnFailure && !callBinding.isTypeCoercionEnabled()) {
                throw callBinding.newError(RESOURCE.mustNotNullInElse());
            }
            return false;
        }
        return true;
    }

    @Override
    public SqlSyntax getSyntax() {
        return SqlCaseOperator.INSTANCE.getSyntax();
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        assert call.getOperandList().size() == 3;

        final SqlWriter.Frame frame =
                writer.startList(SqlWriter.FrameTypeEnum.CASE, "CASE", "END");

        SqlNodeList whenList = (SqlNodeList) call.getOperandList().get(0);
        SqlNodeList thenList = (SqlNodeList) call.getOperandList().get(1);
        assert whenList.size() == thenList.size();
        for (Pair<SqlNode, SqlNode> pair : Pair.zip(whenList, thenList)) {
            writer.sep("WHEN");
            pair.left.unparse(writer, 0, 0);
            writer.sep("THEN");
            pair.right.unparse(writer, 0, 0);
        }

        writer.sep("ELSE");
        SqlNode elseExpr = call.getOperandList().get(2);
        elseExpr.unparse(writer, 0, 0);
        writer.endList(frame);
    }

    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
        return new HazelcastSqlCase(pos, operands[0], (SqlNodeList) operands[1], (SqlNodeList) operands[2], operands[3]);
    }

    private static class CaseReturnTypeInference implements SqlReturnTypeInference {

        @Override
        @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity", "checkstyle:nestedifdepth"})
        public RelDataType inferReturnType(SqlOperatorBinding binding) {
            System.out.println("ThreeOperandCaseReturnTypeInference#inferReturnType");
            // Copied from CalCite with small changes
            SqlCallBinding callBinding = (SqlCallBinding) binding;
            SqlCall sqlCall = callBinding.getCall();
            SqlNodeList thenList = (SqlNodeList) sqlCall.getOperandList().get(2);
            ArrayList<SqlNode> nullList = new ArrayList<>();
            List<RelDataType> argTypes = new ArrayList<>();

            final SqlNodeList whenOperands = (SqlNodeList) sqlCall.getOperandList().get(1);
            final RelDataTypeFactory typeFactory = callBinding.getTypeFactory();

            final int size = thenList.getList().size();
            for (int i = 0; i < size; i++) {
                SqlNode node = thenList.get(i);
                RelDataType type = callBinding.getValidator().deriveType(
                        callBinding.getScope(), node);
                SqlNode operand = whenOperands.get(i);
                if (operand.getKind() == SqlKind.IS_NOT_NULL && type.isNullable()) {
                    SqlBasicCall call = (SqlBasicCall) operand;
                    if (call.getOperandList().get(1).equalsDeep(node, Litmus.IGNORE)) {
                        // We're sure that the type is not nullable if the kind is IS NOT NULL.
                        type = typeFactory.createTypeWithNullability(type, false);
                    }
                }
                argTypes.add(type);
                if (SqlUtil.isNullLiteral(node, false)) {
                    nullList.add(node);
                }
            }

            SqlNode elseOp = sqlCall.getOperandList().get(3);
            argTypes.add(
                    callBinding.getValidator().deriveType(
                            callBinding.getScope(), elseOp));
            if (SqlUtil.isNullLiteral(elseOp, false)) {
                nullList.add(elseOp);
            }

            RelDataType caseReturnType = typeFactory.leastRestrictive(argTypes);
            if (null == caseReturnType) {
                boolean coerced = false;
                if (callBinding.isTypeCoercionEnabled()) {
                    TypeCoercion typeCoercion = callBinding.getValidator().getTypeCoercion();
                    RelDataType commonType = typeCoercion.getWiderTypeFor(argTypes, true);
                    // commonType is always with nullability as false, we do not consider the
                    // nullability when deducing the common type. Use the deduced type
                    // (with the correct nullability) in SqlValidator
                    // instead of the commonType as the return type.
                    if (null != commonType) {
                        coerced = typeCoercion.caseWhenCoercion(callBinding);
                        if (coerced) {
                            caseReturnType = callBinding.getValidator()
                                    .deriveType(callBinding.getScope(), callBinding.getCall());
                        }
                    }
                }
                if (!coerced) {
                    throw callBinding.newValidationError(RESOURCE.illegalMixingOfTypes());
                }
            }
            final SqlValidatorImpl validator =
                    (SqlValidatorImpl) callBinding.getValidator();
            for (SqlNode node : nullList) {
                validator.setValidatedNodeType(node, caseReturnType);
            }
            return caseReturnType;
        }
    }
}
