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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.validate.ValidationUtil;
import com.hazelcast.jet.sql.impl.validate.operand.NamedOperandCheckerProgram;
import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.operand.OperandChecker;
import com.hazelcast.sql.impl.calcite.validate.operand.OperandCheckerProgram;
import com.hazelcast.sql.impl.calcite.validate.operators.common.HazelcastFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.toList;

public abstract class JetTableFunction extends HazelcastFunction {

    private final SqlConnector connector;
    private final List<String> parameterNames;
    private final OperandChecker[] checkers;

    protected JetTableFunction(
            String name,
            List<JetTableFunctionParameter> parameters,
            SqlReturnTypeInference returnTypeInference,
            SqlOperandTypeInference operandTypeInference,
            SqlConnector connector
    ) {
        super(
                name,
                SqlKind.OTHER_FUNCTION,
                returnTypeInference,
                operandTypeInference,
                SqlFunctionCategory.USER_DEFINED_TABLE_SPECIFIC_FUNCTION
        );

        this.connector = connector;
        this.parameterNames = parameters.stream().map(JetTableFunctionParameter::name).collect(toList());
        this.checkers = parameters.stream().map(JetTableFunctionParameter::checker).toArray(OperandChecker[]::new);
    }

    public final boolean isStream() {
        return connector.isStream();
    }

    @Override
    public final List<String> getParamNames() {
        return parameterNames;
    }

    @Override
    protected final boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
        if (ValidationUtil.hasAssignment(binding.getCall())) {
            return new NamedOperandCheckerProgram(checkers).check(binding, throwOnFailure);
        } else {
            OperandChecker[] checkers = Arrays.copyOfRange(this.checkers, 0, binding.getOperandCount());
            return new OperandCheckerProgram(checkers).check(binding, throwOnFailure);
        }
    }
}
