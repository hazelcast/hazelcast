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

package com.hazelcast.sql.impl.calcite.validate.operators.math;

import com.hazelcast.sql.impl.calcite.validate.binding.SqlCallBindingManualOverride;
import com.hazelcast.sql.impl.calcite.validate.binding.SqlCallBindingOverride;
import com.hazelcast.sql.impl.calcite.validate.operand.NumericOperandChecker;
import com.hazelcast.sql.impl.calcite.validate.types.ReplaceUnknownOperandTypeInference;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;

public final class HazelcastSignFunction extends SqlFunction implements SqlCallBindingManualOverride {

    public static final HazelcastSignFunction INSTANCE = new HazelcastSignFunction();

    private HazelcastSignFunction() {
        super(
            "SIGN",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0,
            new ReplaceUnknownOperandTypeInference(BIGINT),
            null,
            SqlFunctionCategory.NUMERIC
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(1);
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding binding, boolean throwOnFailure) {
        SqlCallBindingOverride bindingOverride = new SqlCallBindingOverride(binding);

        return NumericOperandChecker.checkNumericUnknownAsBigint(bindingOverride, throwOnFailure, 0);
    }
}
