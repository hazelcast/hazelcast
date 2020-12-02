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
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlMonotonicUnaryFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.validate.SqlMonotonicity;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastOperandTypes.notAny;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;

public class HazelcastSqlFloorFunction extends SqlMonotonicUnaryFunction {
    public HazelcastSqlFloorFunction(SqlKind kind) {
        super(
            kind.name(),
            kind,
            ReturnTypes.ARG0_OR_EXACT_NO_SCALE,
            new ReplaceUnknownOperandTypeInference(DECIMAL),
            notAny(OperandTypes.NUMERIC),
            SqlFunctionCategory.NUMERIC
        );
    }

    @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
        return call.getOperandMonotonicity(0).unstrict();
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        SqlWriter.Frame frame = writer.startFunCall(getName());

        if (call.operandCount() == 2) {
            call.operand(0).unparse(writer, 0, 100);
            writer.sep("TO");
            call.operand(1).unparse(writer, 100, 0);
        } else {
            call.operand(0).unparse(writer, 0, 0);
        }
        writer.endFunCall(frame);
    }
}
