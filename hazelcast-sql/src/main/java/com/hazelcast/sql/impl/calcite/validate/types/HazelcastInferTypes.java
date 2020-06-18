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

import java.util.Arrays;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.NULL;

public final class HazelcastInferTypes {

    // NOTE: Calcite validator's unknown type has NULL SqlTypeName.

    // The same as Calcite's FIRST_KNOWN, but doesn't consider NULL as a known
    // type.
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

        Arrays.fill(operandTypes, known);
    };

    // The same as FIRST_KNOWN, but widens integer types to BIGINT, so dynamic
    // parameters receive widest integer type.
    public static final SqlOperandTypeInference NUMERIC_FIRST_KNOWN = (binding, returnType, operandTypes) -> {
        RelDataType unknown = binding.getValidator().getUnknownType();

        RelDataType known = unknown;
        for (SqlNode operand : binding.operands()) {
            RelDataType type = binding.getValidator().deriveType(binding.getScope(), operand);

            if (!type.equals(unknown) && type.getSqlTypeName() != NULL) {
                known = type;
                break;
            }
        }

        if (HazelcastIntegerType.supports(known.getSqlTypeName())) {
            known = HazelcastTypeFactory.INSTANCE.createSqlType(BIGINT);
        }

        Arrays.fill(operandTypes, known);
    };

    private HazelcastInferTypes() {
    }

}
