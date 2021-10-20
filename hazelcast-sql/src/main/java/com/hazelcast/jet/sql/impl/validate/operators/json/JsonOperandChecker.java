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

package com.hazelcast.jet.sql.impl.validate.operators.json;

import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.operand.OperandChecker;
import com.hazelcast.jet.sql.impl.validate.operand.TypedOperandChecker;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

public final class JsonOperandChecker implements OperandChecker {
    public static JsonOperandChecker INSTANCE = new JsonOperandChecker();

    private JsonOperandChecker() {
    }

    @Override
    public boolean check(final HazelcastCallBinding callBinding,
                                               final boolean throwOnFailure,
                                               final int operandIndex) {
        final RelDataType operandType = callBinding.getOperandType(operandIndex);
        if (operandType.getSqlTypeName().equals(SqlTypeName.VARCHAR)) {
            return TypedOperandChecker.VARCHAR.check(callBinding, throwOnFailure, operandIndex);
        }

        if (operandType.getSqlTypeName().equals(SqlTypeName.OTHER)) {
            boolean isJson = TypedOperandChecker.JSON.check(callBinding, false, operandIndex) ||
                    TypedOperandChecker.JSON_NULLABLE.check(callBinding, false, operandIndex);
            if (isJson) {
                return true;
            }
        }

        if (throwOnFailure) {
            throw callBinding.newValidationSignatureError();
        } else {
            return false;
        }
    }
}
