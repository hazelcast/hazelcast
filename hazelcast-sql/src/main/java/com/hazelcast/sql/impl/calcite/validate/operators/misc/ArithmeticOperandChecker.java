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

import com.hazelcast.sql.impl.calcite.validate.operand.CompositeOperandChecker;
import com.hazelcast.sql.impl.calcite.validate.operand.TypedOperandChecker;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastIntegerType;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.isNumeric;

public final class ArithmeticOperandChecker {

    public static final ArithmeticOperandChecker INSTANCE = new ArithmeticOperandChecker();

    private ArithmeticOperandChecker() {
        // No-op.
    }

    public boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure, SqlKind kind) {
        RelDataType firstType = binding.getOperandType(0);
        RelDataType secondType = binding.getOperandType(1);

        if (!isNumeric(firstType) || !isNumeric(secondType)) {
            if (throwOnFailure) {
                throw binding.newValidationSignatureError();
            } else {
                return false;
            }
        }

        RelDataType type = HazelcastTypeSystem.withHigherPrecedence(firstType, secondType);

        switch (kind) {
            case PLUS:
            case MINUS:
            case DIVIDE:
                if (HazelcastTypeSystem.isInteger(type)) {
                    int bitWidth = HazelcastIntegerType.bitWidthOf(type) + 1;

                    type = HazelcastIntegerType.of(bitWidth, type.isNullable());
                }

                break;

            default:
                assert kind == SqlKind.TIMES;

                if (HazelcastTypeSystem.isInteger(firstType) && HazelcastTypeSystem.isInteger(secondType)) {
                    int bitWidth = HazelcastIntegerType.bitWidthOf(firstType) + HazelcastIntegerType.bitWidthOf(secondType);

                    type = HazelcastIntegerType.of(bitWidth, type.isNullable());
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
