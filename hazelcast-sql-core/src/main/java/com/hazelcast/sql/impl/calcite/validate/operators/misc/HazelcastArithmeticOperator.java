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

package com.hazelcast.sql.impl.calcite.validate.operators.misc;

import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.operators.BinaryOperatorOperandTypeInference;
import com.hazelcast.sql.impl.calcite.validate.operators.common.HazelcastBinaryOperator;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.validate.SqlMonotonicity;

public final class HazelcastArithmeticOperator extends HazelcastBinaryOperator {

    public static final HazelcastArithmeticOperator PLUS = new HazelcastArithmeticOperator(SqlStdOperatorTable.PLUS);
    public static final HazelcastArithmeticOperator MINUS = new HazelcastArithmeticOperator(SqlStdOperatorTable.MINUS);
    public static final HazelcastArithmeticOperator MULTIPLY = new HazelcastArithmeticOperator(SqlStdOperatorTable.MULTIPLY);
    public static final HazelcastArithmeticOperator DIVIDE = new HazelcastArithmeticOperator(SqlStdOperatorTable.DIVIDE);
    public static final HazelcastArithmeticOperator REMAINDER =
        new HazelcastArithmeticOperator(SqlStdOperatorTable.PERCENT_REMAINDER);

    private HazelcastArithmeticOperator(SqlBinaryOperator base) {
        super(
            base.getName(),
            base.getKind(),
            base.getLeftPrec(),
            true,
            ReturnTypes.ARG0_NULLABLE,
            BinaryOperatorOperandTypeInference.INSTANCE
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(2);
    }

    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.BINARY;
    }

    @Override
    public boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
        return HazelcastArithmeticOperatorUtils.checkOperandTypes(binding, throwOnFailure, kind);
    }

    @Override
    public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
        return SqlMonotonicity.NOT_MONOTONIC;
    }
}
