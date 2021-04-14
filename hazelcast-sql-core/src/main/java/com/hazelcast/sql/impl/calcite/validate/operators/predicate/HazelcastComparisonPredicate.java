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

package com.hazelcast.sql.impl.calcite.validate.operators.predicate;

import com.hazelcast.sql.impl.calcite.validate.operators.BinaryOperatorOperandTypeInference;
import com.hazelcast.sql.impl.calcite.validate.operators.common.HazelcastBinaryOperator;
import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;

public final class HazelcastComparisonPredicate extends HazelcastBinaryOperator {

    public static final HazelcastComparisonPredicate EQUALS = new HazelcastComparisonPredicate(
            SqlStdOperatorTable.EQUALS
    );

    public static final HazelcastComparisonPredicate NOT_EQUALS = new HazelcastComparisonPredicate(
            SqlStdOperatorTable.NOT_EQUALS
    );

    public static final HazelcastComparisonPredicate GREATER_THAN = new HazelcastComparisonPredicate(
            SqlStdOperatorTable.GREATER_THAN
    );

    public static final HazelcastComparisonPredicate GREATER_THAN_OR_EQUAL = new HazelcastComparisonPredicate(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL
    );

    public static final HazelcastComparisonPredicate LESS_THAN = new HazelcastComparisonPredicate(
            SqlStdOperatorTable.LESS_THAN
    );

    public static final HazelcastComparisonPredicate LESS_THAN_OR_EQUAL = new HazelcastComparisonPredicate(
            SqlStdOperatorTable.LESS_THAN_OR_EQUAL
    );

    private HazelcastComparisonPredicate(SqlBinaryOperator base) {
        super(
                base.getName(),
                base.getKind(),
                base.getLeftPrec(),
                true,
                ReturnTypes.BOOLEAN_NULLABLE,
                BinaryOperatorOperandTypeInference.INSTANCE
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(2);
    }

    @Override
    public boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
        return HazelcastComparisonPredicateUtils.checkOperandTypes(binding, throwOnFailure);
    }
}
