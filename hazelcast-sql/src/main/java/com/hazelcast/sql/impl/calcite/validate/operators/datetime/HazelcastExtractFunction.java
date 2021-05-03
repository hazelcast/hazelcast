/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.validate.operators.datetime;

import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.operand.AnyOperandChecker;
import com.hazelcast.sql.impl.calcite.validate.operand.OperandCheckerProgram;
import com.hazelcast.sql.impl.calcite.validate.operand.TypedOperandChecker;
import com.hazelcast.sql.impl.calcite.validate.operators.ReplaceUnknownOperandTypeInference;
import com.hazelcast.sql.impl.calcite.validate.operators.common.HazelcastFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;

public final class HazelcastExtractFunction extends HazelcastFunction {
    public static final HazelcastExtractFunction INSTANCE = new HazelcastExtractFunction();

    private HazelcastExtractFunction() {
        super(
                "EXTRACT",
                SqlKind.EXTRACT,
                ReturnTypes.DOUBLE_NULLABLE,
                new ReplaceUnknownOperandTypeInference(
                        new SqlTypeName[] {
                                null,
                                SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
                        }),
                SqlFunctionCategory.SYSTEM
        );
    }

    @Override
    protected boolean checkOperandTypes(HazelcastCallBinding callBinding, boolean throwOnFailure) {
        return new OperandCheckerProgram(
                    AnyOperandChecker.INSTANCE,
                    TypedOperandChecker.TIMESTAMP_WITH_TIMEZONE
            ).check(callBinding, throwOnFailure);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(2);
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        SqlWriter.Frame frame = writer.startFunCall(getName());

        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.sep("FROM");
        call.operand(1).unparse(writer, leftPrec, rightPrec);

        writer.endFunCall(frame);
    }
}
