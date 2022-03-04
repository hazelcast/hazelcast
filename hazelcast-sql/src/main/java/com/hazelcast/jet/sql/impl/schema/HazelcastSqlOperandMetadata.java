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

import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.ValidationUtil;
import com.hazelcast.jet.sql.impl.validate.operand.NamedOperandCheckerProgram;
import com.hazelcast.jet.sql.impl.validate.operand.OperandChecker;
import com.hazelcast.jet.sql.impl.validate.operand.OperandCheckerProgram;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastOperandTypeCheckerAware;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlOperandTypeInference;

import javax.annotation.Nullable;
import java.util.List;

public abstract class HazelcastSqlOperandMetadata implements SqlOperandMetadata, HazelcastOperandTypeCheckerAware {

    private final List<HazelcastTableFunctionParameter> parameters;
    private final SqlOperandTypeInference operandTypeInference;

    public HazelcastSqlOperandMetadata(List<HazelcastTableFunctionParameter> parameters) {
        this(parameters, null);
    }

    public HazelcastSqlOperandMetadata(
            List<HazelcastTableFunctionParameter> parameters,
            SqlOperandTypeInference operandTypeInference
    ) {
        this.parameters = parameters;
        this.operandTypeInference = operandTypeInference;
    }

    public final List<HazelcastTableFunctionParameter> parameters() {
        return parameters;
    }

    @Override
    public final List<RelDataType> paramTypes(RelDataTypeFactory typeFactory) {
        throw new UnsupportedOperationException("Should not be called");
    }

    @Override
    public final List<String> paramNames() {
        return Util.toList(parameters, HazelcastTableFunctionParameter::name);
    }

    @Override
    public final boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        HazelcastCallBinding binding = prepareBinding(callBinding, operandTypeInference);
        boolean checkResult;
        if (ValidationUtil.hasAssignment(binding.getCall())) {
            OperandChecker[] checkers = parameters.stream()
                    .map(HazelcastTableFunctionParameter::checker)
                    .toArray(OperandChecker[]::new);
            checkResult = new NamedOperandCheckerProgram(checkers).check(binding, throwOnFailure);
        } else {
            OperandChecker[] checkers = parameters.stream()
                    .limit(binding.getOperandCount())
                    .map(HazelcastTableFunctionParameter::checker)
                    .toArray(OperandChecker[]::new);
            checkResult = new OperandCheckerProgram(checkers).check(binding, throwOnFailure);
        }
        return checkResult && checkOperandTypes(binding, throwOnFailure);
    }

    protected abstract boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure);

    @Override
    public final SqlOperandCountRange getOperandCountRange() {
        int numberOfOptionalParameters = (int) parameters.stream().filter(HazelcastTableFunctionParameter::optional).count();
        return SqlOperandCountRanges.between(parameters.size() - numberOfOptionalParameters, parameters.size());
    }

    @Override
    public final String getAllowedSignatures(SqlOperator operator, String operatorName) {
        throw new UnsupportedOperationException("Should not be called");
    }

    @Override
    public final Consistency getConsistency() {
        return Consistency.NONE;
    }

    @Override
    public final boolean isOptional(int i) {
        return parameters.get(i).optional();
    }

    @Nullable
    @Override
    public final SqlOperandTypeInference typeInference() {
        return operandTypeInference;
    }
}
