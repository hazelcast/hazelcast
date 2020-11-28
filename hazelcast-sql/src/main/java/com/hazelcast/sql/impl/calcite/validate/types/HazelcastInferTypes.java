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
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Arrays;
import java.util.Collections;

import static com.hazelcast.sql.impl.calcite.validate.SqlNodeUtil.isParameter;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.typeName;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.NULL;

/**
 * A collection of operand type inference strategies. Basically, a mirror of
 * {@link InferTypes} provided by Calcite with various enhancements.
 */
public final class HazelcastInferTypes {

    /**
     * The same as Calcite's {@link InferTypes#FIRST_KNOWN}, but doesn't consider
     * NULL as a known type and widens integer types to BIGINT for dynamic parameters.
     */
    public static final SqlOperandTypeInference FIRST_KNOWN = (binding, returnType, operandTypes) -> {
        // NOTE: Calcite validator's unknown type has NULL SqlTypeName.

        RelDataType unknown = binding.getValidator().getUnknownType();

        boolean seenParameters = false;
        RelDataType known = unknown;
        for (int i = 0; i < binding.getOperandCount(); ++i) {
            RelDataType type = binding.getOperandType(i);

            if (isParameter(binding.operand(i))) {
                seenParameters = true;
            } else if (known.equals(unknown) && typeName(type) != NULL) {
                known = type;
            }
        }

        if (seenParameters && HazelcastIntegerType.supports(typeName(known))) {
            known = HazelcastTypeFactory.INSTANCE.createSqlType(BIGINT);
        }

        Arrays.fill(operandTypes, known);
    };

    /**
     * The same as Calcite's {@link InferTypes#ANY_NULLABLE}, but doesn't assign
     * OBJECT/ANY type to NULLs.
     */
    public static final SqlOperandTypeInference NULLABLE_OBJECT = (binding, returnType, operandTypes) -> {
        // NOTE: Calcite validator's unknown type has NULL SqlTypeName.

        assert binding.getTypeFactory() == HazelcastTypeFactory.INSTANCE;
        RelDataType unknown = binding.getValidator().getUnknownType();
        for (int i = 0; i < operandTypes.length; ++i) {
            RelDataType type = binding.getOperandType(i);

            if (unknown.equals(type) || typeName(type) != NULL) {
                operandTypes[i] = HazelcastObjectType.NULLABLE_INSTANCE;
            }
        }
    };

    private HazelcastInferTypes() {
        // No-op.
    }

    /**
     * Inference strategy that replaces the first operand of the call binding with the provided type, or does nothing if
     * the call has more than one operand.
     *
     * @param typeName type to be used for the first operand
     * @return inference strategy
     */
    public static SqlOperandTypeInference explicitSingle(SqlTypeName typeName) {
        return InferTypes.explicit(Collections.singletonList(HazelcastTypeFactory.INSTANCE.createSqlType(typeName)));
    }
}
