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

import com.hazelcast.jet.sql.impl.schema.HazelcastTableFunctionParameter;
import com.hazelcast.jet.sql.impl.validate.ValidationUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.hazelcast.jet.sql.impl.validate.ValidatorResource.RESOURCE;
import static com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils.createNullableType;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class HazelcastOperandTypeInference implements SqlOperandTypeInference {

    private final Map<String, HazelcastTableFunctionParameter> parametersByName;
    private final SqlOperandTypeInference positionalOperandTypeInference;

    public HazelcastOperandTypeInference(
            List<HazelcastTableFunctionParameter> parameters,
            SqlOperandTypeInference positionalOperandTypeInference
    ) {
        this.parametersByName = parameters.stream().collect(toMap(HazelcastTableFunctionParameter::name, identity()));
        this.positionalOperandTypeInference = positionalOperandTypeInference;
    }

    @Override
    public void inferOperandTypes(SqlCallBinding callBinding, RelDataType returnType, RelDataType[] operandTypes) {
        SqlCall call = callBinding.getCall();
        if (ValidationUtil.hasAssignment(call)) {
            RelDataTypeFactory typeFactory = callBinding.getTypeFactory();
            RelDataType[] parameterTypes = new RelDataType[parametersByName.size()];
            for (int i = 0; i < call.operandCount(); i++) {
                SqlCall assignment = call.operand(i);
                SqlIdentifier id = assignment.operand(1);
                String name = id.getSimple();

                HazelcastTableFunctionParameter parameter = parametersByName.get(name);
                if (parameter != null) {
                    SqlTypeName parameterType = parameter.type();
                    parameterTypes[parameter.ordinal()] = toType(parameterType, typeFactory);
                } else {
                    throw SqlUtil.newContextException(id.getParserPosition(), RESOURCE.unknownArgumentName(name));
                }
            }
            //noinspection ResultOfMethodCallIgnored
            Arrays.stream(parameterTypes).filter(Objects::nonNull).toArray(ignored -> operandTypes);
        } else {
            positionalOperandTypeInference.inferOperandTypes(callBinding, returnType, operandTypes);
        }
    }

    private static RelDataType toType(SqlTypeName parameterType, RelDataTypeFactory typeFactory) {
        if (parameterType == SqlTypeName.MAP) {
            RelDataType sqlType = typeFactory.createUnknownType();
            return typeFactory.createMapType(sqlType, sqlType);
        } else {
            RelDataType sqlType = typeFactory.createSqlType(parameterType);
            return sqlType.isNullable() ? createNullableType(typeFactory, sqlType) : sqlType;
        }
    }
}
