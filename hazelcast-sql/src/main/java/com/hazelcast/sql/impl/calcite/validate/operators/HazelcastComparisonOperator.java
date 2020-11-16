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

import com.hazelcast.sql.impl.calcite.validate.types.HazelcastInferTypes;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastOperandTypes;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastOperandTypes.notAny;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastOperandTypes.wrap;

public final class HazelcastComparisonOperator extends SqlBinaryOperator {
    public static final HazelcastComparisonOperator EQUALS = new HazelcastComparisonOperator(
        SqlStdOperatorTable.EQUALS,
        false
    );

    public static final HazelcastComparisonOperator NOT_EQUALS = new HazelcastComparisonOperator(
        SqlStdOperatorTable.NOT_EQUALS,
        false
    );

    public static final HazelcastComparisonOperator GREATER_THAN = new HazelcastComparisonOperator(
        SqlStdOperatorTable.GREATER_THAN,
        false
    );

    public static final HazelcastComparisonOperator GREATER_THAN_OR_EQUAL = new HazelcastComparisonOperator(
        SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
        false
    );

    public static final HazelcastComparisonOperator LESS_THAN = new HazelcastComparisonOperator(
        SqlStdOperatorTable.LESS_THAN,
        false
    );

    public static final HazelcastComparisonOperator LESS_THAN_OR_EQUAL = new HazelcastComparisonOperator(
        SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
        false
    );

    private HazelcastComparisonOperator(SqlBinaryOperator base, boolean requiresComparable) {
        super(
            base.getName(),
            base.getKind(),
            base.getLeftPrec(),
            true,
            ReturnTypes.BOOLEAN_NULLABLE,
            HazelcastInferTypes.FIRST_KNOWN,
            requiresComparable ? wrap(notAny(HazelcastOperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED))
                : wrap(notAny(OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED))
        );
    }

    // TODO: Enable later
//    @Override
//    public SqlOperandCountRange getOperandCountRange() {
//        return SqlOperandCountRanges.of(2);
//    }
}
