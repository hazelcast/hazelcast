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

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlCase;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
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
import org.apache.calcite.sql.fun.SqlCaseOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
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
        final HazelcastSqlCase sqlCase = (HazelcastSqlCase) call;

        final SqlNodeList whenOperands = sqlCase.getWhenOperands();
        for (SqlNode operand : whenOperands) {
            operand.validateExpr(validator, operandScope);
        }

        final SqlNodeList thenOperands = sqlCase.getThenOperands();
        for (SqlNode operand : thenOperands) {
            operand.validateExpr(validator, operandScope);
        }

        final SqlNode elseOperand = sqlCase.getElseOperand();
        if (elseOperand != null) {
            elseOperand.validateExpr(validator, operandScope);
        }
    }

    @Override
    public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
        // SqlCaseOperator is doing the same
        return validateOperands(validator, scope, call);
    }

    @Override
    // override this methods because passing null into constructor for SqlOperandTypeChecker
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.any();
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        SqlNodeList whenList = (SqlNodeList) callBinding.getCall().getOperandList().get(1);
        SqlNodeList thenList = (SqlNodeList) callBinding.getCall().getOperandList().get(2);
        SqlNode elseOperand = callBinding.getCall().getOperandList().get(3);
        assert whenList.size() == thenList.size();

        for (SqlNode node : whenList) {
            RelDataType type = callBinding.getValidator().deriveType(callBinding.getScope(), node);
            // TODO: add test
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
            if (type.getSqlTypeName() != SqlTypeName.NULL) {
                foundNotNull = true;
            }
        }

        RelDataType elseType = callBinding.getValidator().deriveType(callBinding.getScope(), elseOperand);
        if (elseType.getSqlTypeName() != SqlTypeName.NULL) {
            foundNotNull = true;
        }

        // TODO: add test
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

        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.CASE, "CASE", "END");

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
            // Copied from CalCite with small changes
            SqlCallBinding callBinding = (SqlCallBinding) binding;
            SqlCall sqlCall = callBinding.getCall();
            SqlValidator validator = callBinding.getValidator();

            SqlNodeList thenList = (SqlNodeList) sqlCall.getOperandList().get(2);
            List<SqlNode> nullList = new ArrayList<>();
            List<RelDataType> argTypes = new ArrayList<>();

            final SqlNodeList whenOperands = (SqlNodeList) sqlCall.getOperandList().get(1);
            final RelDataTypeFactory typeFactory = callBinding.getTypeFactory();

            final int size = thenList.getList().size();
            for (int i = 0; i < size; i++) {
                SqlNode node = thenList.get(i);
                RelDataType type = validator.deriveType(callBinding.getScope(), node);
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

            List<SqlNode> allReturnNodes = new ArrayList<>(thenList.getList());
            SqlNode elseOp = sqlCall.getOperandList().get(3);
            allReturnNodes.add(elseOp);
            argTypes.add(validator.deriveType(callBinding.getScope(), elseOp));
            if (SqlUtil.isNullLiteral(elseOp, false)) {
                nullList.add(elseOp);
            }

            RelDataType caseReturnType = typeFactory.leastRestrictive(argTypes);
            if (null == caseReturnType) {
                RelDataType highType = argTypes.get(0);
                for (int i = 1; i < argTypes.size(); i++) {
                    highType = HazelcastTypeUtils.withHigherPrecedence(highType, argTypes.get(i));
                }

                QueryDataType highHZType = HazelcastTypeUtils.toHazelcastType(highType.getSqlTypeName());

                boolean canConvert = true;
                for (int i = 0, argTypesSize = argTypes.size(); i < argTypesSize; i++) {
                    RelDataType type = argTypes.get(i);
                    QueryDataType hzType = HazelcastTypeUtils.toHazelcastType(type.getSqlTypeName());

                    SqlNode sqlNode = allReturnNodes.get(i);
                    canConvert &= bothParametersAreNumeric(highHZType, hzType)
                            || bothOperandsAreTemporalAndLowOperandCanBeConvertedToHighOperand(highHZType, hzType)
                            || highOperandIsTemporalAndLowOperandIsLiteralOfVarcharType(highHZType, hzType, sqlNode);
                }

                if (!canConvert) {
                    throw QueryException.error(SqlErrorCode.GENERIC, "Cannot infer return type for CASE among " + argTypes);
                }
                caseReturnType = highType;
            }

            final SqlValidatorImpl sqlValidator = (SqlValidatorImpl) validator;
            for (SqlNode node : nullList) {
                sqlValidator.setValidatedNodeType(node, caseReturnType);
            }
            return caseReturnType;
        }

        private static boolean bothParametersAreNumeric(QueryDataType highHZType, QueryDataType lowHZType) {
            return (highHZType.getTypeFamily().isNumeric() && lowHZType.getTypeFamily().isNumeric());
        }

        private static boolean bothOperandsAreTemporalAndLowOperandCanBeConvertedToHighOperand(QueryDataType highHZType,
                                                                                               QueryDataType lowHZType) {
            return highHZType.getTypeFamily().isTemporal()
                    && lowHZType.getTypeFamily().isTemporal()
                    && lowHZType.getConverter().canConvertTo(highHZType.getTypeFamily());
        }

        private static boolean highOperandIsTemporalAndLowOperandIsLiteralOfVarcharType(QueryDataType highHZType,
                                                                                        QueryDataType lowHZType, SqlNode low) {
            return highHZType.getTypeFamily().isTemporal()
                    && lowHZType.getTypeFamily() == QueryDataTypeFamily.VARCHAR
                    && low instanceof SqlLiteral;
        }
    }
}
