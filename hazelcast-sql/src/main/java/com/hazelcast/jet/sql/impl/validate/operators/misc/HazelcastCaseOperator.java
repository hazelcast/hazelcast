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

import com.hazelcast.jet.sql.impl.validate.HazelcastResources;
import com.hazelcast.jet.sql.impl.validate.HazelcastSqlValidator;
import com.hazelcast.jet.sql.impl.validate.operators.special.HazelcastSqlCase;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.CalciteContextException;
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
import org.apache.calcite.sql.fun.SqlCaseOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.hazelcast.jet.sql.impl.validate.operators.typeinference.HazelcastReturnTypeInference.wrap;
import static com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils.isNullOrUnknown;
import static org.apache.calcite.util.Static.RESOURCE;

public final class HazelcastCaseOperator extends SqlOperator {
    public static final HazelcastCaseOperator INSTANCE = new HazelcastCaseOperator();

    private static final int ELSE_BRANCH_OPERAND_INDEX = 3;

    private HazelcastCaseOperator() {
        super("CASE", SqlKind.CASE, SqlOperator.MDX_PRECEDENCE, true, wrap(new CaseReturnTypeInference()), null, null);
    }

    @Override
    public void validateCall(SqlCall call, SqlValidator validator, SqlValidatorScope scope, SqlValidatorScope operandScope) {
        SqlCaseOperator.INSTANCE.validateCall(call, validator, scope, operandScope);
    }

    @Override
    public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
        preValidateCall(validator, scope, call);

        SqlCallBinding opBinding = new SqlCallBinding(validator, scope, call);

        checkOperandTypes(opBinding, true);

        return inferReturnType(opBinding);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.any();
    }

    @Override
    @SuppressWarnings("checkstyle:MethodLength")
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        HazelcastSqlValidator validator = (HazelcastSqlValidator) callBinding.getValidator();
        HazelcastSqlCase sqlCall = (HazelcastSqlCase) callBinding.getCall();
        // at this point `CASE x WHEN y ...` is already converted to `CASE WHEN x=y ...`
        assert sqlCall.getValueOperand() == null;
        SqlNodeList whenList = sqlCall.getWhenOperands();
        SqlNodeList thenList = sqlCall.getThenOperands();
        SqlNode elseOperand = sqlCall.getElseOperand();

        assert whenList.size() > 0 : "no WHEN clause";
        assert whenList.size() == thenList.size();

        SqlValidatorScope scope = callBinding.getScope();

        if (!typeCheckWhen(scope, validator, whenList)) {
            if (throwOnFailure) {
                throw callBinding.newError(RESOURCE.expectedBoolean());
            }
            return false;
        }

        List<RelDataType> argTypes = new ArrayList<>(thenList.size() + 1);
        for (SqlNode node : thenList) {
            argTypes.add(validator.deriveType(scope, node));
        }
        argTypes.add(validator.deriveType(scope, elseOperand));
        //noinspection OptionalGetWithoutIsPresent
        RelDataType caseReturnType = argTypes.stream().reduce(HazelcastTypeUtils::withHigherPrecedence).get();

        Supplier<CalciteContextException> exceptionSupplier = () ->
                validator.newValidationError(
                        sqlCall, HazelcastResources.RESOURCES.cannotInferCaseResult(argTypes.toString(), "CASE"));

        for (int i = 0; i < thenList.size(); i++) {
            int finalI = i;
            if (!coerceItem(
                    validator,
                    scope,
                    thenList.get(i),
                    caseReturnType,
                    sqlNode -> thenList.getList().set(finalI, sqlNode),
                    throwOnFailure,
                    exceptionSupplier)
            ) {
                return false;
            }
        }

        return coerceItem(
                validator,
                scope,
                elseOperand,
                caseReturnType,
                sqlNode -> sqlCall.setOperand(ELSE_BRANCH_OPERAND_INDEX, sqlNode),
                throwOnFailure,
                exceptionSupplier);
    }

    private boolean coerceItem(
            HazelcastSqlValidator validator,
            SqlValidatorScope scope,
            SqlNode item,
            RelDataType type,
            Consumer<SqlNode> replaceFn,
            boolean throwOnFailure,
            Supplier<CalciteContextException> exceptionSupplier) {
        boolean elementTypeCoerced = validator.getTypeCoercion().rowTypeElementCoercion(
                scope,
                item,
                type,
                replaceFn);

        if (!elementTypeCoerced) {
            if (throwOnFailure) {
                throw exceptionSupplier.get();
            } else {
                return false;
            }
        }
        return true;
    }

    private boolean typeCheckWhen(SqlValidatorScope scope, HazelcastSqlValidator validator, SqlNodeList whenList) {
        for (SqlNode node : whenList) {
            RelDataType type = validator.deriveType(scope, node);
            if (isNullOrUnknown(type.getSqlTypeName())) {
                type = validator.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN);
                validator.setValidatedNodeType(node, type);
            }
            if (!SqlTypeUtil.inBooleanFamily(type)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public SqlSyntax getSyntax() {
        return SqlCaseOperator.INSTANCE.getSyntax();
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        assert call instanceof HazelcastSqlCase;

        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.CASE, "CASE", "END");
        HazelcastSqlCase sqlCase = (HazelcastSqlCase) call;

        SqlNodeList whenList = sqlCase.getWhenOperands();
        SqlNodeList thenList = sqlCase.getThenOperands();
        assert whenList.size() == thenList.size();
        for (Pair<SqlNode, SqlNode> pair : Pair.zip(whenList, thenList)) {
            writer.sep("WHEN");
            pair.left.unparse(writer, 0, 0);
            writer.sep("THEN");
            pair.right.unparse(writer, 0, 0);
        }

        writer.sep("ELSE");
        SqlNode elseExpr = sqlCase.getElseOperand();
        elseExpr.unparse(writer, 0, 0);
        writer.endList(frame);
    }

    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
        return new HazelcastSqlCase(pos, operands[0], (SqlNodeList) operands[1], (SqlNodeList) operands[2], operands[3]);
    }

    private static class CaseReturnTypeInference implements SqlReturnTypeInference {

        @Override
        public RelDataType inferReturnType(SqlOperatorBinding binding) {
            return binding.getOperandType(ELSE_BRANCH_OPERAND_INDEX);
        }
    }
}
