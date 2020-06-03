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

package com.hazelcast.sql.impl.calcite.validate.types;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.NULL;

public final class HazelcastInferTypes {

    // NOTE: Calcite validator's unknown type has NULL SqlTypeName.

    public static final SqlOperandTypeInference FIRST_KNOWN = (binding, returnType, operandTypes) -> {
        RelDataType unknown = binding.getValidator().getUnknownType();

        RelDataType known = unknown;
        for (SqlNode operand : binding.operands()) {
            RelDataType type = binding.getValidator().deriveType(binding.getScope(), operand);

            if (!type.equals(unknown) && type.getSqlTypeName() != NULL) {
                known = type;
                break;
            }
        }

        for (int i = 0; i < operandTypes.length; ++i) {
            RelDataType type = binding.getOperandType(i);
            if (type.getSqlTypeName() == NULL && !unknown.equals(type)) {
                // keep NULLs as NULLs
                operandTypes[i] = type;
            } else {
                operandTypes[i] = known;
            }
        }
    };

    public static final SqlOperandTypeInference NUMERIC = (binding, returnType, operandTypes) -> {
        RelDataType unknown = binding.getValidator().getUnknownType();

        RelDataType known = unknown;
        boolean seenInteger = false;
        boolean seenDecimal = false;

        for (SqlNode operand : binding.operands()) {
            RelDataType type = binding.getValidator().deriveType(binding.getScope(), operand);

            if (!type.equals(unknown) && type.getSqlTypeName() != NULL) {
                // We are happy with any known type, but we need to look for
                // numeric types: the loop continues.
                known = type;
            }

            switch (type.getSqlTypeName()) {
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                    seenInteger = true;
                    break;
                case DECIMAL:
                    seenDecimal = true;
                    break;
                default:
                    // do nothing
                    break;
            }
        }

        RelDataType inferred;
        if (seenDecimal) {
            inferred = HazelcastTypeFactory.INSTANCE.createSqlType(DECIMAL);
        } else if (seenInteger) {
            inferred = HazelcastTypeFactory.INSTANCE.createSqlType(BIGINT);
        } else {
            inferred = known;
        }

        for (int i = 0; i < operandTypes.length; ++i) {
            RelDataType type = binding.getOperandType(i);
            if (type.getSqlTypeName() == NULL && !unknown.equals(type)) {
                // keep NULLs as NULLs
                operandTypes[i] = type;
            } else {
                operandTypes[i] = inferred;
            }
        }
    };

    public static final SqlOperandTypeInference BOOLEAN = (binding, returnType, operandTypes) -> {
        RelDataType unknown = binding.getValidator().getUnknownType();

        for (int i = 0; i < operandTypes.length; ++i) {
            RelDataType type = binding.getOperandType(i);
            if (type.getSqlTypeName() == NULL && !unknown.equals(type)) {
                // keep NULLs as NULLs
                operandTypes[i] = type;
            } else {
                operandTypes[i] = HazelcastTypeFactory.INSTANCE.createSqlType(SqlTypeName.BOOLEAN, type.isNullable());
            }
        }
    };

    private HazelcastInferTypes() {
    }

}
