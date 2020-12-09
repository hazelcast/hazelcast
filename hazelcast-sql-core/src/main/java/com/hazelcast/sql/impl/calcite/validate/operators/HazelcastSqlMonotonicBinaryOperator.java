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

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlMonotonicBinaryOperator;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlMonotonicity;

/**
 * The same as {@link SqlMonotonicBinaryOperator}, but supports monotonicity for
 * NULL literals.
 */
public final class HazelcastSqlMonotonicBinaryOperator extends SqlMonotonicBinaryOperator {

    public HazelcastSqlMonotonicBinaryOperator(String name, SqlKind kind, int prec, boolean isLeftAssoc,
                                               SqlReturnTypeInference returnTypeInference,
                                               SqlOperandTypeInference operandTypeInference,
                                               SqlOperandTypeChecker operandTypeChecker) {
        super(name, kind, prec, isLeftAssoc, returnTypeInference, operandTypeInference, operandTypeChecker);
    }

    @Override
    public SqlMonotonicity getMonotonicity(SqlOperatorBinding binding) {
        if (binding.isOperandNull(0, true) || binding.isOperandNull(1, true)) {
            return SqlMonotonicity.CONSTANT;
        }

        return super.getMonotonicity(binding);
    }

}
