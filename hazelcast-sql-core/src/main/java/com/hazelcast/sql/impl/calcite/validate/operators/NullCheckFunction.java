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
import com.hazelcast.sql.impl.calcite.validate.operand.TypedOperandChecker;
import com.hazelcast.sql.impl.calcite.validate.operators.common.HazelcastFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.predicate.HazelcastComparisonPredicateUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import static com.hazelcast.sql.impl.calcite.validate.operators.HazelcastReturnTypeInference.wrap;

public class NullCheckFunction extends HazelcastFunction {
    public static final NullCheckFunction NULL_IF = new NullCheckFunction("NULLIF", SqlKind.NULLIF, SqlFunctionCategory.SYSTEM);
    public static final NullCheckFunction COALESCE = new NullCheckFunction("COALESCE", SqlKind.COALESCE, SqlFunctionCategory.SYSTEM);

    private NullCheckFunction(String name, SqlKind kind, SqlFunctionCategory category) {
        super(name, kind, wrap(new NullCheckReturnTypeInference()), BinaryOperatorOperandTypeInference.INSTANCE, category);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.any();
    }

    @Override
    protected boolean checkOperandTypes(HazelcastCallBinding callBinding, boolean throwOnFailure) {
        return HazelcastComparisonPredicateUtils.checkOperandTypes(callBinding, throwOnFailure);
    }

    private static class NullCheckReturnTypeInference implements SqlReturnTypeInference {
        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            return opBinding.getOperandType(0);
        }
    }
}
