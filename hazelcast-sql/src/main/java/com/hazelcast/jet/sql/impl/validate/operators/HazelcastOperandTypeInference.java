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

package com.hazelcast.jet.sql.impl.validate.operators;

import com.hazelcast.jet.sql.impl.validate.ValidationUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils.createNullableType;

public class HazelcastOperandTypeInference implements SqlOperandTypeInference {

    private final SqlTypeName[] namedOperandTypes;
    private final SqlOperandTypeInference positionalOperandTypeInference;

    public HazelcastOperandTypeInference(
            SqlTypeName[] namedOperandTypes,
            SqlOperandTypeInference positionalOperandTypeInference
    ) {
        this.namedOperandTypes = namedOperandTypes;
        this.positionalOperandTypeInference = positionalOperandTypeInference;
    }

    @Override
    public void inferOperandTypes(SqlCallBinding callBinding, RelDataType returnType, RelDataType[] operandTypes) {
        if (ValidationUtil.hasAssignment(callBinding.getCall())) {
            assert namedOperandTypes.length == operandTypes.length;

            RelDataTypeFactory typeFactory = callBinding.getTypeFactory();
            for (int i = 0; i < operandTypes.length; i++) {
                RelDataType operandType = typeFactory.createSqlType(namedOperandTypes[i]);

                operandTypes[i] = operandType.isNullable() ? createNullableType(typeFactory, operandType) : operandType;
            }
        } else {
            positionalOperandTypeInference.inferOperandTypes(callBinding, returnType, operandTypes);
        }
    }
}
