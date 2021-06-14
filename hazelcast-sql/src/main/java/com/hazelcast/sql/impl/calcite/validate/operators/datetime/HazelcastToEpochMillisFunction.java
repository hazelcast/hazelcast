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

package com.hazelcast.sql.impl.calcite.validate.operators.datetime;

import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.operators.ReplaceUnknownOperandTypeInference;
import com.hazelcast.sql.impl.calcite.validate.operators.common.HazelcastFunction;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;

public final class HazelcastToEpochMillisFunction extends HazelcastFunction {
    public static final HazelcastToEpochMillisFunction INSTANCE = new HazelcastToEpochMillisFunction();

    private HazelcastToEpochMillisFunction() {
        super(
                "TO_EPOCH_MILLIS",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.explicit(SqlTypeName.BIGINT),
                new ReplaceUnknownOperandTypeInference(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE),
                SqlFunctionCategory.SYSTEM
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(1);
    }

    @Override
    public boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
        final RelDataType operandType = binding.getOperandType(0);
        final boolean correct = SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE.equals(operandType.getSqlTypeName());

        if (throwOnFailure && !correct) {
            throw binding.newValidationSignatureError();
        } else {
            return correct;
        }
    }
}
