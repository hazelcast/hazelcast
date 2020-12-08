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

package com.hazelcast.sql.impl.calcite.validate.operators.misc;

import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils;
import com.hazelcast.sql.impl.calcite.validate.operand.CompositeOperandChecker;
import com.hazelcast.sql.impl.calcite.validate.operand.TypedOperandChecker;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastIntegerType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;

public final class ArithmeticOperandChecker {

    public static final ArithmeticOperandChecker INSTANCE = new ArithmeticOperandChecker();

    private ArithmeticOperandChecker() {
        // No-op.
    }

    public boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure, SqlKind kind) {
        RelDataType firstType = binding.getOperandType(0);
        RelDataType secondType = binding.getOperandType(1);

        if (!HazelcastTypeUtils.isNumericType(firstType) || !HazelcastTypeUtils.isNumericType(secondType)) {
            if (throwOnFailure) {
                throw binding.newValidationSignatureError();
            } else {
                return false;
            }
        }

        RelDataType type = HazelcastTypeUtils.withHigherPrecedence(firstType, secondType);

        switch (kind) {
            case PLUS:
            case MINUS:
            case DIVIDE:
                if (HazelcastTypeUtils.isNumericIntegerType(type)) {
                    int bitWidth = ((HazelcastIntegerType) type).getBitWidth() + 1;

                    type = HazelcastIntegerType.create(bitWidth, type.isNullable());
                }

                break;

            default:
                assert kind == SqlKind.TIMES;

                if (HazelcastTypeUtils.isNumericIntegerType(firstType) && HazelcastTypeUtils.isNumericIntegerType(secondType)) {
                    int bitWidth = ((HazelcastIntegerType) firstType).getBitWidth()
                        + ((HazelcastIntegerType) secondType).getBitWidth();

                    type = HazelcastIntegerType.create(bitWidth, type.isNullable());
                }

                break;
        }

        TypedOperandChecker checker = TypedOperandChecker.forType(type);

        return new CompositeOperandChecker(
            checker,
            checker
        ).check(binding, throwOnFailure);
    }
}
