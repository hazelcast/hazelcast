/*
 * Copyright 2024 Hazelcast Inc.
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
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastFunction;
import com.hazelcast.jet.sql.impl.validate.operators.misc.HazelcastArithmeticOperator;
import com.hazelcast.jet.sql.impl.validate.operators.misc.HazelcastArithmeticOperatorUtils;
import com.hazelcast.jet.sql.impl.validate.operators.typeinference.BinaryOperatorOperandTypeInference;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.SqlOperandCountRanges;

public final class HazelcastModFunction extends HazelcastFunction {

    public static final HazelcastModFunction MOD = new HazelcastModFunction();

    private HazelcastModFunction() {
        super(
                "MOD",
                SqlKind.MOD,
                new HazelcastArithmeticOperator.ArithmeticTypeInference(),
                BinaryOperatorOperandTypeInference.INSTANCE,
                SqlFunctionCategory.NUMERIC
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(2);
    }

    @Override
    public boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
        return HazelcastArithmeticOperatorUtils.checkOperandTypes(binding, throwOnFailure, kind);
    }
}
