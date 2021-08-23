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

package com.hazelcast.sql.impl.calcite.validate.operators.json;

import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.operand.TypedOperandChecker;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

public final class JsonFunctionUtil {
    private JsonFunctionUtil() { }
    public static boolean checkJsonOperandType(final HazelcastCallBinding callBinding,
                                               final boolean throwOnFailure,
                                               final int operandIndex) {
        final RelDataType operandType = callBinding.getOperandType(operandIndex);
        if (operandType.getSqlTypeName().equals(SqlTypeName.VARCHAR)) {
            return TypedOperandChecker.VARCHAR.check(callBinding, throwOnFailure, operandIndex);
        }

        if (operandType.getSqlTypeName().equals(SqlTypeName.OTHER)) {
            return TypedOperandChecker.JSON.check(callBinding, throwOnFailure, operandIndex);
        }

        if (throwOnFailure) {
            throw callBinding.newValidationSignatureError();
        } else {
            return false;
        }
    }
}
