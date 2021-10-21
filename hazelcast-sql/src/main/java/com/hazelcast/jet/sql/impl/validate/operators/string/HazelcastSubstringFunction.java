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

import com.google.common.collect.ImmutableList;
import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.operand.OperandCheckerProgram;
import com.hazelcast.jet.sql.impl.validate.operand.TypedOperandChecker;
import com.hazelcast.jet.sql.impl.validate.operators.typeinference.ReplaceUnknownOperandTypeInference;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastFunction;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;

import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

public final class HazelcastSubstringFunction extends HazelcastFunction {

    public static final HazelcastSubstringFunction INSTANCE = new HazelcastSubstringFunction();

    private HazelcastSubstringFunction() {
        super(
                "SUBSTRING",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.ARG0_NULLABLE_VARYING,
                new ReplaceUnknownOperandTypeInference(new SqlTypeName[]{VARCHAR, INTEGER, INTEGER}),
                SqlFunctionCategory.STRING
        );
    }

    @Override
    public String getSignatureTemplate(int operandsCount) {
        switch (operandsCount) {
            case 2:
                return "{0}({1} FROM {2})";
            case 3:
                return "{0}({1} FROM {2} FOR {3})";
            default:
                throw new AssertionError();
        }
    }

    @Override
    public String getAllowedSignatures(String opName) {
        StringBuilder ret = new StringBuilder();
        for (Ord<SqlTypeName> typeName : Ord.zip(SqlTypeName.CHAR_TYPES)) {
            if (typeName.i > 0) {
                ret.append(NL);
            }

            ret.append(
                    SqlUtil.getAliasedSignature(this, opName, ImmutableList.of(typeName.e, SqlTypeName.INTEGER))
            );
            ret.append(NL);
            ret.append(
                    SqlUtil.getAliasedSignature(this, opName, ImmutableList.of(typeName.e, SqlTypeName.INTEGER,
                            SqlTypeName.INTEGER))
            );
        }
        return ret.toString();
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
                    TypedOperandChecker.INTEGER
            ).check(binding, throwOnFailure);
        } else {
            assert binding.getOperandCount() == 3;

            return new OperandCheckerProgram(
                    TypedOperandChecker.VARCHAR,
                    TypedOperandChecker.INTEGER,
                    TypedOperandChecker.INTEGER
            ).check(binding, throwOnFailure);
        }
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        SqlWriter.Frame frame = writer.startFunCall(getName());

        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.sep("FROM");
        call.operand(1).unparse(writer, leftPrec, rightPrec);

        if (call.operandCount() == 3) {
            writer.sep("FOR");
            call.operand(2).unparse(writer, leftPrec, rightPrec);
        }

        writer.endFunCall(frame);
    }
}
