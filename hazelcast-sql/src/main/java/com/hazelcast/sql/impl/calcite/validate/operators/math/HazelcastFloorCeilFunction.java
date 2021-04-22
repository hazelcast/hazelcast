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

package com.hazelcast.sql.impl.calcite.validate.operators.math;

import com.hazelcast.sql.impl.calcite.validate.operand.NumericOperandChecker;
import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.operators.common.HazelcastFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.ReplaceUnknownOperandTypeInference;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.validate.SqlMonotonicity;

import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;

public final class HazelcastFloorCeilFunction extends HazelcastFunction {

    public static final SqlFunction FLOOR = new HazelcastFloorCeilFunction(SqlKind.FLOOR);
    public static final SqlFunction CEIL = new HazelcastFloorCeilFunction(SqlKind.CEIL);

    private HazelcastFloorCeilFunction(SqlKind kind) {
        super(
                kind.name(),
                kind,
                ReturnTypes.ARG0_OR_EXACT_NO_SCALE,
                new ReplaceUnknownOperandTypeInference(DECIMAL),
                SqlFunctionCategory.NUMERIC
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(1);
    }

    @Override
    public boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
        return NumericOperandChecker.INSTANCE.check(binding, throwOnFailure, 0);
    }

    @Override
    public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
        return call.getOperandMonotonicity(0).unstrict();
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
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
