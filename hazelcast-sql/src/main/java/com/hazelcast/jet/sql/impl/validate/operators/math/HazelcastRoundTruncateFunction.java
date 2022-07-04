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

package com.hazelcast.jet.sql.impl.validate.operators.math;

import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.operand.NumericOperandChecker;
import com.hazelcast.jet.sql.impl.validate.operand.OperandCheckerProgram;
import com.hazelcast.jet.sql.impl.validate.operand.TypedOperandChecker;
import com.hazelcast.jet.sql.impl.validate.operators.typeinference.ReplaceUnknownOperandTypeInference;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;

import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;

public final class HazelcastRoundTruncateFunction extends HazelcastFunction {

    public static final HazelcastRoundTruncateFunction ROUND = new HazelcastRoundTruncateFunction("ROUND");
    public static final HazelcastRoundTruncateFunction TRUNCATE = new HazelcastRoundTruncateFunction("TRUNCATE");

    private HazelcastRoundTruncateFunction(String name) {
        super(
                name,
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.ARG0_NULLABLE,
                new ReplaceUnknownOperandTypeInference(new SqlTypeName[]{DECIMAL, INTEGER}),
                SqlFunctionCategory.NUMERIC
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.between(1, 2);
    }

    @Override
    public boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
        if (binding.getOperandCount() == 1) {
            return NumericOperandChecker.INSTANCE.check(binding, throwOnFailure, 0);
        } else {
            assert binding.getOperandCount() == 2;

            return new OperandCheckerProgram(
                    NumericOperandChecker.INSTANCE,
                    TypedOperandChecker.INTEGER
            ).check(binding, throwOnFailure);
        }
    }
}
