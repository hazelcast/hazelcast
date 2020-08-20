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

import com.hazelcast.sql.impl.calcite.validate.types.ReplaceUnknownOperandTypeInference;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeUtil;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastOperandTypes.notAny;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

public class HazelcastSqlLikeOperator extends SqlSpecialOperator {

    private static final int PRECEDENCE = 32;

    public HazelcastSqlLikeOperator() {
        super(
            "LIKE",
            SqlKind.LIKE,
            PRECEDENCE,
            false,
            ReturnTypes.BOOLEAN_NULLABLE,
            new ReplaceUnknownOperandTypeInference(VARCHAR),
            null
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.between(2, 3);
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        int operandCount = callBinding.getOperandCount();

        assert operandCount == 2 || operandCount == 3;

        SqlSingleOperandTypeChecker operandTypeChecker = operandCount == 2
            ? OperandTypes.STRING_SAME_SAME : OperandTypes.STRING_SAME_SAME_SAME;

        operandTypeChecker = notAny(operandTypeChecker);

        if (!operandTypeChecker.checkOperandTypes(callBinding, throwOnFailure)) {
            return false;
        }

        return SqlTypeUtil.isCharTypeComparable(callBinding, callBinding.operands(), throwOnFailure);
    }
}
