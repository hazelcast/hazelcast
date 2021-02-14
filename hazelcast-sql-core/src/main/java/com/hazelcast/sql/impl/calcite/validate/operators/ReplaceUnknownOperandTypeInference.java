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

package com.hazelcast.sql.impl.calcite.validate.operators;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils.createNullableType;

/**
 * Type inference that replaces UNKNOWN and NULL operands with the given types.
 */
public class ReplaceUnknownOperandTypeInference implements SqlOperandTypeInference {

    private final SqlTypeName[] typeNames;

    /** Type name to be applied to operands that do not have a concrete types defined in "typeNames" */
    private final SqlTypeName defaultTypeName;

    public ReplaceUnknownOperandTypeInference(SqlTypeName[] typeNames) {
        this(typeNames, null);
    }

    public ReplaceUnknownOperandTypeInference(SqlTypeName defaultTypeName) {
        this(null, defaultTypeName);
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public ReplaceUnknownOperandTypeInference(SqlTypeName[] typeNames, SqlTypeName defaultTypeName) {
        this.typeNames = typeNames;
        this.defaultTypeName = defaultTypeName;
    }

    @Override
    public void inferOperandTypes(SqlCallBinding callBinding, RelDataType returnType, RelDataType[] operandTypes) {
        for (int i = 0; i < operandTypes.length; i++) {
            RelDataType operandType = callBinding.getOperandType(i);

            if (operandType.getSqlTypeName() == SqlTypeName.NULL) {
                // note that this path is taken if operandType is both UNKNOWN or NULL.
                // That is in case of FUNCTION(?) and FUNCTION(null).
                RelDataType resolvedOperandType = resolveOperandType(i, callBinding.getTypeFactory());

                // Preserve nullability
                if (operandType.isNullable()) {
                    resolvedOperandType = createNullableType(callBinding.getTypeFactory(), resolvedOperandType);
                }

                operandType = resolvedOperandType;
            }

            operandTypes[i] = operandType;
        }
    }

    private RelDataType resolveOperandType(int index, RelDataTypeFactory typeFactory) {
        SqlTypeName typeName = null;

        if (typeNames != null && index < typeNames.length) {
            typeName = typeNames[index];
        }

        if (typeName == null) {
            typeName = defaultTypeName;
        }

        assert typeName != null;

        return typeFactory.createSqlType(typeName);
    }
}
