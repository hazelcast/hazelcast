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

package com.hazelcast.jet.sql.impl.validate;

import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.validate.SqlValidatorException;

/**
 * Error messages for parsing and validation stages.
 */
public interface HazelcastResources {

    HazelcastResources RESOURCES = Resources.create(HazelcastResources.class);

    @Resources.BaseMessage("Function ''{0}'' does not exist")
    Resources.ExInst<CalciteException> functionDoesNotExist(String functionName);

    @Resources.BaseMessage("Cannot apply {0} operator to {1} (consider adding an explicit CAST)")
    Resources.ExInst<SqlValidatorException> invalidOperatorOperands(String operatorName, String operandTypes);

    @Resources.BaseMessage("Cannot apply {0} function to {1} (consider adding an explicit CAST)")
    Resources.ExInst<SqlValidatorException> invalidFunctionOperands(String functionName, String operandTypes);

    @Resources.BaseMessage("CAST function cannot convert value of type {0} to type {1}")
    Resources.ExInst<SqlValidatorException> cannotCastValue(String sourceType, String targetType);

    @Resources.BaseMessage("CAST function cannot convert literal {0} to type {1}: {2}")
    Resources.ExInst<SqlValidatorException> cannotCastLiteralValue(String sourceValue, String targetType, String message);

    @Resources.BaseMessage("Cannot infer return type for {1} among {0}")
    Resources.ExInst<SqlValidatorException> cannotInferCaseResult(String types, String operator);

    @Resources.BaseMessage("The descriptor column type ({0}) and the interval type ({1}) do not match")
    Resources.ExInst<SqlValidatorException> windowFunctionTypeMismatch(String descriptorType, String intervalType);
}
