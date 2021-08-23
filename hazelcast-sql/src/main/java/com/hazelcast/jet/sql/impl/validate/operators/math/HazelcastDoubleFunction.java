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
import com.hazelcast.jet.sql.impl.validate.operand.TypedOperandChecker;
import com.hazelcast.jet.sql.impl.validate.operators.typeinference.ReplaceUnknownOperandTypeInference;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;

import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;

/**
 * Function that accepts a DOUBLE argument and produces a DOUBLE result.
 */
public class HazelcastDoubleFunction extends HazelcastFunction {
    public HazelcastDoubleFunction(String name) {
        super(
                name,
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.DOUBLE_NULLABLE,
                new ReplaceUnknownOperandTypeInference(DOUBLE),
                SqlFunctionCategory.NUMERIC
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(1);
    }

    @Override
    public boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
        return TypedOperandChecker.DOUBLE.check(binding, throwOnFailure, 0);
    }
}
