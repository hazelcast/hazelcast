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

import com.hazelcast.sql.impl.calcite.validate.types.ReplaceUnknownOperandTypeInference;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlLikeOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeUtil;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastOperandTypes.notAny;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

public class HazelcastSqlLikeOperator extends SqlSpecialOperator {

    private static final int PRECEDENCE = 32;

    public HazelcastSqlLikeOperator() {
        super(
            "LIKE",
            SqlKind.LIKE,
            PRECEDENCE,
            false,
            ReturnTypes.BOOLEAN_NULLABLE,
            new ReplaceUnknownOperandTypeInference(VARCHAR),
            null
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.between(2, 3);
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        int operandCount = callBinding.getOperandCount();

        assert operandCount == 2 || operandCount == 3;

        SqlSingleOperandTypeChecker operandTypeChecker = operandCount == 2
            ? OperandTypes.STRING_SAME_SAME : OperandTypes.STRING_SAME_SAME_SAME;

        operandTypeChecker = notAny(operandTypeChecker);

        if (!operandTypeChecker.checkOperandTypes(callBinding, throwOnFailure)) {
            return false;
        }

        return SqlTypeUtil.isCharTypeComparable(callBinding, callBinding.operands(), throwOnFailure);
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        SqlWriter.Frame frame = writer.startList("", "");

        call.operand(0).unparse(writer, getLeftPrec(), getRightPrec());

        writer.sep(getName());

        call.operand(1).unparse(writer, getLeftPrec(), getRightPrec());

        if (call.operandCount() == 3) {
            writer.sep("ESCAPE");

            call.operand(2).unparse(writer, getLeftPrec(), getRightPrec());
        }

        writer.endList(frame);
    }

    @Override
    public ReduceResult reduceExpr(int opOrdinal, TokenSequence list) {
        SqlNode exp0 = list.node(opOrdinal - 1);

        SqlOperator op = list.op(opOrdinal);

        assert op instanceof SqlLikeOperator;

        SqlNode exp1 = SqlParserUtil.toTreeEx(list, opOrdinal + 1, getRightPrec(), SqlKind.ESCAPE);
        SqlNode exp2 = null;

        if ((opOrdinal + 2) < list.size()) {
            if (list.isOp(opOrdinal + 2)) {
                SqlOperator op2 = list.op(opOrdinal + 2);

                if (op2.getKind() == SqlKind.ESCAPE) {
                    exp2 = SqlParserUtil.toTreeEx(list, opOrdinal + 3, getRightPrec(), SqlKind.ESCAPE);
                }
            }
        }
        SqlNode[] operands;

        int end;

        if (exp2 != null) {
            operands = new SqlNode[]{exp0, exp1, exp2};
            end = opOrdinal + 4;
        } else {
            operands = new SqlNode[]{exp0, exp1};
            end = opOrdinal + 2;
        }

        SqlCall call = createCall(SqlParserPos.ZERO, operands);

        return new ReduceResult(opOrdinal - 1, end, call);
    }
}
