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

package com.hazelcast.jet.sql.impl.validate.operators.predicate;

import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.operators.typeinference.ComparisonOperatorOperandTypeInference;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastBinaryOperator;
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
                ComparisonOperatorOperandTypeInference.INSTANCE
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
