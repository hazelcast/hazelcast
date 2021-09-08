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
import com.hazelcast.jet.sql.impl.validate.operators.typeinference.ReplaceUnknownOperandTypeInference;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;

public class HazelcastPositionFunction extends HazelcastFunction {
    public static final HazelcastPositionFunction INSTANCE = new HazelcastPositionFunction();

    public HazelcastPositionFunction() {
        super(
                "POSITION",
                SqlKind.POSITION,
                ReturnTypes.INTEGER_NULLABLE,
                new ReplaceUnknownOperandTypeInference(
                        new SqlTypeName[] {
                                SqlTypeName.VARCHAR,
                                SqlTypeName.VARCHAR,
                                SqlTypeName.INTEGER
                        }),
                SqlFunctionCategory.NUMERIC
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.between(2, 3);
    }

    @Override
    protected boolean checkOperandTypes(HazelcastCallBinding callBinding, boolean throwOnFailure) {
        switch (callBinding.getOperandCount()) {
            case 2:
                return new OperandCheckerProgram(
                        TypedOperandChecker.VARCHAR,
                        TypedOperandChecker.VARCHAR
                ).check(callBinding, throwOnFailure);
            case 3:
                return new OperandCheckerProgram(
                        TypedOperandChecker.VARCHAR,
                        TypedOperandChecker.VARCHAR,
                        TypedOperandChecker.INTEGER
                ).check(callBinding, throwOnFailure);
            default:
                return false;
        }
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        SqlWriter.Frame frame = writer.startFunCall(getName());

        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.sep("IN");
        call.operand(1).unparse(writer, leftPrec, rightPrec);

        if (call.operandCount() == 3) {
            writer.sep("FROM");
            call.operand(2).unparse(writer, leftPrec, rightPrec);
        }

        writer.endFunCall(frame);
    }
}
