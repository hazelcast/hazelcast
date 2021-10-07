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

import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.ValidationUtil;
import com.hazelcast.jet.sql.impl.validate.operand.NamedOperandCheckerProgram;
import com.hazelcast.jet.sql.impl.validate.operand.OperandChecker;
import com.hazelcast.jet.sql.impl.validate.operand.OperandCheckerProgram;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastOperandTypeCheckerAware;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlOperandTypeInference;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public final class HazelcastSqlOperandMetadata implements SqlOperandMetadata, HazelcastOperandTypeCheckerAware {

    private final List<HazelcastTableFunctionParameter> parameters;
    private final SqlOperandTypeInference operandTypeInference;

    public HazelcastSqlOperandMetadata(
            List<HazelcastTableFunctionParameter> parameters,
            SqlOperandTypeInference operandTypeInference
    ) {
        this.parameters = parameters;
        this.operandTypeInference = operandTypeInference;
    }

    public List<HazelcastTableFunctionParameter> parameters() {
        return parameters;
    }

    @Override
    public List<RelDataType> paramTypes(RelDataTypeFactory typeFactory) {
        throw new UnsupportedOperationException("Should not be called");
    }

    @Override
    public List<String> paramNames() {
        return parameters.stream()
                .map(HazelcastTableFunctionParameter::name)
                .collect(toList());
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        HazelcastCallBinding binding = prepareBinding(callBinding, operandTypeInference);
        if (ValidationUtil.hasAssignment(binding.getCall())) {
            OperandChecker[] checkers = parameters.stream()
                    .map(HazelcastTableFunctionParameter::checker)
                    .toArray(OperandChecker[]::new);
            return new NamedOperandCheckerProgram(checkers).check(binding, throwOnFailure);
        } else {
            OperandChecker[] checkers = parameters.stream()
                    .limit(binding.getOperandCount())
                    .map(HazelcastTableFunctionParameter::checker)
                    .toArray(OperandChecker[]::new);
            return new OperandCheckerProgram(checkers).check(binding, throwOnFailure);
        }
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        int numberOfOptionalParameters = (int) parameters.stream().filter(HazelcastTableFunctionParameter::optional).count();
        return SqlOperandCountRanges.between(parameters.size() - numberOfOptionalParameters, parameters.size());
    }

    @Override
    public String getAllowedSignatures(SqlOperator operator, String operatorName) {
        return parameters.stream()
                .map(parameter -> {
                    QueryDataType type = HazelcastTypeUtils.toHazelcastType(parameter.type());
                    return parameter.optional()
                            ? "[, " + parameter.name() + " " + type.getTypeFamily() + "]"
                            : parameter.name() + " " + type.getTypeFamily();
                }).collect(Collectors.joining("", operatorName + "(", ")"));
    }

    @Override
    public Consistency getConsistency() {
        return Consistency.NONE;
    }

    @Override
    public boolean isOptional(int i) {
        return parameters.get(i).optional();
    }

    @Nonnull
    @Override
    public SqlOperandTypeInference typeInference() {
        return operandTypeInference;
    }
}
