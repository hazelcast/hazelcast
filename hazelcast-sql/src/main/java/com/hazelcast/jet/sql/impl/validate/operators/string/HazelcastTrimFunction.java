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
import com.hazelcast.jet.sql.impl.validate.HazelcastCallBindingSignatureErrorAware;
import com.hazelcast.jet.sql.impl.validate.literal.LiteralUtils;
import com.hazelcast.jet.sql.impl.validate.operand.AnyOperandChecker;
import com.hazelcast.jet.sql.impl.validate.operand.OperandCheckerProgram;
import com.hazelcast.jet.sql.impl.validate.operand.TypedOperandChecker;
import com.hazelcast.jet.sql.impl.validate.operators.typeinference.ReplaceUnknownOperandTypeInference;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * Our own implementation of the TRIM function that has custom operand type inference to allow for dynamic parameters.
 * <p>
 * Code of some methods is copy-pasted from the Calcite's {@link SqlTrimFunction}, because it is not extensible enough.
 */
public final class HazelcastTrimFunction extends HazelcastFunction implements HazelcastCallBindingSignatureErrorAware {

    public static final HazelcastTrimFunction INSTANCE = new HazelcastTrimFunction();

    private HazelcastTrimFunction() {
        super(
                "TRIM",
                SqlKind.TRIM,
                ReturnTypes.ARG2_NULLABLE,
                new ReplaceUnknownOperandTypeInference(SqlTypeName.VARCHAR),
                SqlFunctionCategory.STRING
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(3);
    }

    @Override
    public boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
        if (binding.getOperandCount() == 2) {
            return new OperandCheckerProgram(
                    AnyOperandChecker.INSTANCE,
                    TypedOperandChecker.VARCHAR
            ).check(binding, throwOnFailure);
        } else {
            assert binding.getOperandCount() == 3;

            return new OperandCheckerProgram(
                    AnyOperandChecker.INSTANCE,
                    TypedOperandChecker.VARCHAR,
                    TypedOperandChecker.VARCHAR
            ).check(binding, throwOnFailure);
        }
    }

    @Override
    public Collection<SqlNode> getOperandsForSignatureError(SqlCall call) {
        SqlNode fromOperand = call.operand(1);
        SqlNode targetOperand = call.operand(2);

        SqlTypeName literalType = LiteralUtils.literalTypeName(fromOperand);

        if (literalType == SqlTypeName.VARCHAR && " ".equals(((SqlLiteral) fromOperand).getValueAs(String.class))) {
            // Default value for the FROM operand, report only target operand.
            return Collections.singletonList(targetOperand);
        }

        // Non-default FROM, report both target and FROM operands.
        return Arrays.asList(fromOperand, targetOperand);
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        SqlWriter.Frame frame = writer.startFunCall(getName());

        assert call.operand(0) instanceof SqlLiteral : call.operand(0);

        call.operand(0).unparse(writer, leftPrec, rightPrec);
        call.operand(1).unparse(writer, leftPrec, rightPrec);
        writer.sep("FROM");
        call.operand(2).unparse(writer, leftPrec, rightPrec);
        writer.endFunCall(frame);
    }

    @Override
    public String getSignatureTemplate(final int operandsCount) {
        assert operandsCount == 3;

        return "{0}([BOTH|LEADING|TRAILING] {1} FROM {2})";
    }

    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
        switch (operands.length) {
            case 1:
                // This variant occurs when someone writes TRIM(string)
                // as opposed to the sugared syntax TRIM(string FROM string).
                operands = new SqlNode[]{
                        SqlTrimFunction.Flag.BOTH.symbol(SqlParserPos.ZERO),
                        SqlLiteral.createCharString(" ", pos),
                        operands[0]
                };

                break;

            case 3:
                assert operands[0] instanceof SqlLiteral && ((SqlLiteral) operands[0]).getValue() instanceof SqlTrimFunction.Flag;

                if (operands[1] == null) {
                    operands[1] = SqlLiteral.createCharString(" ", pos);
                }

                break;

            default:
                throw new IllegalArgumentException("invalid operand count " + Arrays.toString(operands));
        }

        return super.createCall(functionQualifier, pos, operands);
    }
}
