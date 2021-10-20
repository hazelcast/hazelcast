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

package com.hazelcast.jet.sql.impl.validate.operators.typeinference;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

public final class JsonFunctionOperandTypeInference implements SqlOperandTypeInference {

    @Override
    public void inferOperandTypes(final SqlCallBinding callBinding,
                                  final RelDataType returnType,
                                  final RelDataType[] operandTypes) {
        for (int i = 0; i < callBinding.getOperandCount(); i++) {
            operandTypes[i] = callBinding.getOperandType(i);
            if (operandTypes[i].getSqlTypeName() == SqlTypeName.NULL) {
                operandTypes[i] = callBinding.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
            }
        }

        if (operandTypes[0].getSqlTypeName() == SqlTypeName.NULL) {
            operandTypes[0] = callBinding.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
        }
    }
}
