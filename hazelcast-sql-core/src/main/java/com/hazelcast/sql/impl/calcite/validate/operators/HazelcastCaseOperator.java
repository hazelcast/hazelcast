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
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlCaseOperator;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Pair;

import java.util.Arrays;

import static com.hazelcast.sql.impl.calcite.validate.operators.HazelcastReturnTypeInference.wrap;

public final class HazelcastCaseOperator extends SqlOperator {

    public static final HazelcastCaseOperator INSTANCE = new HazelcastCaseOperator();

    private HazelcastCaseOperator() {
        super(SqlCaseOperator.INSTANCE.getName(), SqlKind.CASE, SqlCaseOperator.INSTANCE.getLeftPrec(), true,
                wrap(new CaseReturnTypeInference()), new CaseOperandTypeInference(), null);
    }

    @Override
    public void validateCall(SqlCall call, SqlValidator validator, SqlValidatorScope scope, SqlValidatorScope operandScope) {
        System.out.println("HazelcastCaseOperator#validateCall");
        super.validateCall(call, validator, scope, operandScope);
    }

    @Override
    public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
        // SqlCaseOperator is doing the same
        System.out.println("HazelcastCaseOperator#deriveType");
        return validateOperands(validator, scope, call);
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        System.out.println("HazelcastCaseOperator#checkOperandTypes");
        return super.checkOperandTypes(callBinding, throwOnFailure);
    }

    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.FUNCTION;
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        SqlCase sqlCase = (SqlCase) call;
        final SqlWriter.Frame frame =
                writer.startList(SqlWriter.FrameTypeEnum.CASE, "CASE", "END");
        SqlNodeList whenList = sqlCase.getWhenOperands();
        SqlNodeList thenList = sqlCase.getThenOperands();
        assert whenList.size() == thenList.size();
        if (sqlCase.getValueOperand() != null) {
            sqlCase.getValueOperand().unparse(writer, 0, 0);
        }
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

    private static class CaseOperandTypeInference implements SqlOperandTypeInference {
        @Override
        public void inferOperandTypes(SqlCallBinding callBinding, RelDataType returnType, RelDataType[] operandTypes) {
            System.out.println("CaseOperandTypeInference#inferOperandTypes");
            // operandTypes[0] - all `WHEN` conditions. `CASE <value> WHEN <other value> ...` is rewritten as `CASE WHEN <value> = <other value> ...`
            // operandTypes[1] - all `THEN` expressions.
            // operandTypes[2] - `ELSE` branch.
            assert operandTypes.length == 3;
            assert callBinding.getCall().getOperandList().size() == 3;

            operandTypes[0] = whenBranchesType(callBinding, (SqlNodeList) callBinding.getCall().getOperandList().get(0));
            operandTypes[1] = thenBranchesType(callBinding, (SqlNodeList) callBinding.getCall().getOperandList().get(1));
            operandTypes[2] = elseExpression(callBinding, callBinding.getCall().getOperandList().get(2));
        }

        private RelDataType whenBranchesType(SqlCallBinding callBinding, SqlNodeList whenBranches) {
            SqlValidator validator = callBinding.getValidator();

            for (SqlNode branch : whenBranches) {
                RelDataType relDataType = validator.deriveType(callBinding.getScope(), branch);
            }

            return null;
        }

        private RelDataType thenBranchesType(SqlCallBinding callBinding, SqlNodeList thenBranches) {
            return null;
        }

        private RelDataType elseExpression(SqlCallBinding callBinding, SqlNode elseExpr) {
            return null;
        }
    }

    private static class CaseReturnTypeInference implements SqlReturnTypeInference {

        @Override
        public RelDataType inferReturnType(SqlOperatorBinding binding) {
            System.out.println("CaseReturnTypeInference#inferReturnType");
            int size = binding.getOperandCount();
            RelDataType caseReturnType = binding.getOperandType(1);
            QueryDataType firstThenBranchType = HazelcastTypeUtils.toHazelcastType(caseReturnType.getSqlTypeName());
            QueryDataTypeFamily[] allReturnTypes = new QueryDataTypeFamily[size / 2 + 1];
            int j = 0;
            allReturnTypes[j] = firstThenBranchType.getTypeFamily();
            j++;
            boolean failure = false;
            for (int i = 1 + 2; i < size; i += 2) {
                QueryDataType operandType = HazelcastTypeUtils.toHazelcastType(binding.getOperandType(i).getSqlTypeName());
                failure |= !firstThenBranchType.equals(operandType);
                allReturnTypes[j++] = operandType.getTypeFamily();
            }
            QueryDataType elseType = HazelcastTypeUtils.toHazelcastType(binding.getOperandType(size - 1).getSqlTypeName());
            failure |= !firstThenBranchType.equals(elseType);
            allReturnTypes[j] = elseType.getTypeFamily();
            if (failure) {
                throw QueryException.error(
                        SqlErrorCode.GENERIC,
                        "Cannot infer return type of case operator among " + Arrays.toString(allReturnTypes));
            }
            return caseReturnType;
        }
    }
}
