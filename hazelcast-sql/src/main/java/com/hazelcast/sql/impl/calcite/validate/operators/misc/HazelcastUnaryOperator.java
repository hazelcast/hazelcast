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
import com.hazelcast.sql.impl.calcite.validate.operand.TypedOperandChecker;
import com.hazelcast.sql.impl.calcite.validate.operators.ReplaceUnknownOperandTypeInference;
import com.hazelcast.sql.impl.calcite.validate.operators.common.HazelcastPrefixOperator;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastIntegerType;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;

public final class HazelcastUnaryOperator extends HazelcastPrefixOperator {

    public static final HazelcastUnaryOperator PLUS = new HazelcastUnaryOperator(SqlStdOperatorTable.UNARY_PLUS, false);
    public static final HazelcastUnaryOperator MINUS = new HazelcastUnaryOperator(SqlStdOperatorTable.UNARY_MINUS, true);

    private final boolean extend;

    private HazelcastUnaryOperator(
            SqlPrefixOperator base,
            boolean extend
    ) {
        super(
                base.getName(),
                base.getKind(),
                base.getLeftPrec(),
                ReturnTypes.ARG0,
                new ReplaceUnknownOperandTypeInference(BIGINT)
        );

        this.extend = extend;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(1);
    }

    @Override
    public boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
        RelDataType operandType = binding.getOperandType(0);

        if (HazelcastTypeUtils.isNumericIntegerType(operandType) && extend) {
            int bitWidth = ((HazelcastIntegerType) operandType).getBitWidth();

            operandType = HazelcastIntegerType.create(bitWidth + 1, operandType.isNullable());
        }

        TypedOperandChecker checker = TypedOperandChecker.forType(operandType);

        if (checker.isNumeric()) {
            return checker.check(binding, throwOnFailure, 0);
        }

        if (throwOnFailure) {
            throw binding.newValidationSignatureError();
        } else {
            return false;
        }
    }
}
