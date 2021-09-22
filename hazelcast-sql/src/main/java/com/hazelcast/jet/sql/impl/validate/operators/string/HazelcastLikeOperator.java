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

package com.hazelcast.jet.sql.impl.validate.operators.string;

import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.operand.OperandCheckerProgram;
import com.hazelcast.jet.sql.impl.validate.operand.TypedOperandChecker;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastSpecialOperator;
import com.hazelcast.jet.sql.impl.validate.operators.typeinference.ReplaceUnknownOperandTypeInference;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlLikeOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;

import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

@SuppressWarnings("checkstyle:MagicNumber")
public final class HazelcastLikeOperator extends HazelcastSpecialOperator {

    public static final HazelcastLikeOperator LIKE = new HazelcastLikeOperator("LIKE", SqlKind.LIKE, false);
    // We can't use SqlKind.LIKE for NOT LIKE because it will be handled in RexSimplify#simplifyLike and NOT operator
    // will be lost. In _vanilla_ Calcite it is not an issue as NOT LIKE '%' is converted to NOT(LIKE '%') - see
    // StandardConvertletTable.
    public static final HazelcastLikeOperator NOT_LIKE = new HazelcastLikeOperator("NOT LIKE", SqlKind.OTHER, true);

    private static final int PRECEDENCE = 32;

    private final boolean negated;

    private HazelcastLikeOperator(String name, SqlKind kind, boolean negated) {
        super(
                name,
                kind,
                PRECEDENCE,
                false,
                ReturnTypes.BOOLEAN_NULLABLE,
                new ReplaceUnknownOperandTypeInference(VARCHAR)
        );

        this.negated = negated;
    }

    public boolean isNegated() {
        return negated;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.between(2, 3);
    }

    @Override
    public boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
        if (binding.getOperandCount() == 2) {
            return new OperandCheckerProgram(
                    TypedOperandChecker.VARCHAR,
                    TypedOperandChecker.VARCHAR
            ).check(binding, throwOnFailure);
        } else {
            assert binding.getOperandCount() == 3;

            return new OperandCheckerProgram(
                    TypedOperandChecker.VARCHAR,
                    TypedOperandChecker.VARCHAR,
                    TypedOperandChecker.VARCHAR
            ).check(binding, throwOnFailure);
        }
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
