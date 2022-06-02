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

package com.hazelcast.jet.sql.impl.validate.operators.common;

import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.HazelcastSqlValidator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlOperandTypeInference;

import java.util.Arrays;

import static com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils.isNullOrUnknown;

/**
 * The special interface that provides a utility method to perform a recursive operand type inference before
 * checking the operand types. Without this logic, many expressions will fail to resolve their operand types.
 * See the {@code NestingAndCasingExpressionTest} test: if the recursive inference is skipped, many tests
 * from this class will fail.
 * <p>
 * In addition, this class provides the custom {@link SqlCallBinding} implementation that should be used by
 * operand checker of all operators. This overridden binding {@link HazelcastCallBinding} provides a custom
 * error message for signature errors.
 * <p>
 * All operators must extend this interface and call the {@link #prepareBinding(SqlCallBinding)} method before
 * proceeding with operand type checking. To simplify this task, we provide a set of abstract classes:
 * {@link HazelcastFunction}, {@link HazelcastPrefixOperator}, {@link HazelcastPostfixOperator},
 * {@link HazelcastBinaryOperator}, {@link HazelcastSpecialOperator}.
 */
public interface HazelcastOperandTypeCheckerAware {

    default HazelcastCallBinding prepareBinding(SqlCallBinding binding) {
        SqlOperator operator = binding.getOperator();

        assert operator == this;

        // Resolve unknown types if needed.
        SqlOperandTypeInference operandTypeInference = operator.getOperandTypeInference();

        return prepareBinding(binding, operandTypeInference);
    }

    default HazelcastCallBinding prepareBinding(SqlCallBinding binding, SqlOperandTypeInference operandTypeInference) {
        HazelcastSqlValidator validator = (HazelcastSqlValidator) binding.getValidator();

        boolean resolveOperands = false;

        for (int i = 0; i < binding.getOperandCount(); i++) {
            RelDataType operandType = binding.getOperandType(i);

            if (isNullOrUnknown(operandType.getSqlTypeName())) {
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
                validator.setValidatedNodeType(binding.getCall().operand(i), operandTypes[i]);
            }
        }

        // Provide custom binding
        return new HazelcastCallBinding(binding);
    }
}
