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

package com.hazelcast.sql.impl.calcite.validate.operators;

import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.operators.common.HazelcastFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.predicate.HazelcastComparisonPredicateUtils;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;

import static com.hazelcast.sql.impl.calcite.validate.operators.HazelcastReturnTypeInference.wrap;

public final class NullIfFunction extends HazelcastFunction {
    public static final NullIfFunction INSTANCE = new NullIfFunction();

    private NullIfFunction() {
        super(
                "NULLIF",
                SqlKind.NULLIF,
                wrap(ReturnTypes.ARG0_NULLABLE),
                BinaryOperatorOperandTypeInference.INSTANCE,
                SqlFunctionCategory.SYSTEM
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(2);
    }

    @Override
    protected boolean checkOperandTypes(HazelcastCallBinding callBinding, boolean throwOnFailure) {
        return HazelcastComparisonPredicateUtils.checkOperandTypes(callBinding, throwOnFailure);
    }
}
