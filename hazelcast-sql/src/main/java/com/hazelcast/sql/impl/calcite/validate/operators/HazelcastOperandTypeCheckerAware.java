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

package com.hazelcast.sql.impl.calcite.validate.operators;

import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Arrays;

public interface HazelcastOperandTypeCheckerAware {
    default HazelcastCallBinding prepareBinding(SqlCallBinding binding) {
        SqlOperator operator = binding.getOperator();

        assert operator == this;

        // Resolve unknown types if needed.
        SqlOperandTypeInference operandTypeInference = operator.getOperandTypeInference();

        HazelcastSqlValidator validator = (HazelcastSqlValidator) binding.getValidator();

        boolean resolveOperands = false;

        for (int i = 0; i < binding.getOperandCount(); i++) {
            RelDataType operandType = binding.getOperandType(i);

            if (operandType.getSqlTypeName() == SqlTypeName.NULL) {
                resolveOperands = true;

                break;
            }
        }

        if (resolveOperands) {
            RelDataType unknownType = binding.getValidator().getUnknownType();

            RelDataType[] operandTypes = new RelDataType[binding.getOperandCount()];
            Arrays.fill(operandTypes, unknownType);

            operandTypeInference.inferOperandTypes(binding, binding.getValidator().getUnknownType(), operandTypes);

            for (int i = 0; i < binding.getOperandCount(); i++) {
                validator.setValidatedNodeType(binding.operand(i), operandTypes[i]);
            }
        }

        // Provide custom binding
        return new HazelcastCallBinding(binding);
    }
}
