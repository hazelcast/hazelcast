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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.sql.impl.QueryException;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.util.NlsString;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static org.apache.calcite.sql.SqlKind.MAP_VALUE_CONSTRUCTOR;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.MAP;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

public final class JetSqlUserDefinedTableFunction extends SqlUserDefinedTableFunction {

    public JetSqlUserDefinedTableFunction(
            SqlIdentifier opName,
            SqlReturnTypeInference returnTypeInference,
            SqlOperandTypeInference operandTypeInference,
            SqlOperandTypeChecker operandTypeChecker,
            List<RelDataType> parameterTypes,
            JetDynamicTableFunction function
    ) {
        super(opName, returnTypeInference, operandTypeInference, operandTypeChecker, parameterTypes, function);

        for (RelDataType parameterType : parameterTypes) {
            SqlTypeName type = parameterType.getSqlTypeName();
            checkTrue(type == INTEGER || type == VARCHAR || type == MAP, "Unsupported type: " + type);
        }
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory, List<SqlNode> operands) {
        List<Object> arguments = toArguments(typeFactory, getNameAsId(), getFunction().getParameters(), operands);
        return getFunction().getRowType(typeFactory, arguments);
    }

    private static List<Object> toArguments(
            RelDataTypeFactory typeFactory,
            SqlIdentifier name,
            List<FunctionParameter> parameters,
            List<SqlNode> operands
    ) {
        assert parameters.size() == operands.size();

        List<Object> arguments = new ArrayList<>(parameters.size());
        for (int i = 0; i < parameters.size(); i++) {
            SqlNode operand = operands.get(i);
            FunctionParameter parameter = parameters.get(i);
            Object value = extractValue(typeFactory, name, parameter, operand);
            arguments.add(value);
        }
        return arguments;
    }

    private static Object extractValue(
            RelDataTypeFactory typeFactory,
            SqlIdentifier functionName,
            FunctionParameter parameter,
            SqlNode node
    ) {
        if (node.getKind() == SqlKind.DEFAULT) {
            return null;
        }
        if (SqlUtil.isNullLiteral(node, true)) {
            return null;
        }

        SqlTypeName parameterType = parameter.getType(typeFactory).getSqlTypeName();
        if (SqlUtil.isLiteral(node) && parameterType == INTEGER) {
            Integer value = extractIntegerValue(((SqlLiteral) node));
            if (value != null) {
                return value;
            }
        } else if (SqlUtil.isLiteral(node) && parameterType == VARCHAR) {
            String value = extractStringValue(((SqlLiteral) node));
            if (value != null) {
                return value;
            }
        } else if (node.getKind() == MAP_VALUE_CONSTRUCTOR && parameterType == MAP) {
            return extractMapValue(functionName, parameter, (SqlCall) node);
        }
        throw QueryException.error("Invalid argument of a call to function " + functionName + " - #"
                + parameter.getOrdinal() + " (" + parameter.getName() + "). Expected: " + parameterType
                + ", actual: "
                + (SqlUtil.isLiteral(node) ? ((SqlLiteral) node).getTypeName() : node.getKind()));
    }

    private static Integer extractIntegerValue(SqlLiteral literal) {
        Object value = literal.getValue();
        return value instanceof BigDecimal ? ((BigDecimal) value).intValue() : null;
    }

    private static String extractStringValue(SqlLiteral literal) {
        Object value = literal.getValue();
        return value instanceof NlsString ? ((NlsString) value).getValue() : null;
    }

    private static Map<String, String> extractMapValue(
            SqlIdentifier functionName,
            FunctionParameter parameter,
            SqlCall call
    ) {
        List<SqlNode> operands = call.getOperandList();
        Map<String, String> entries = new HashMap<>();
        for (int i = 0; i < operands.size(); i += 2) {
            String key = extractMapLiteralValue(functionName, parameter, operands.get(i));
            String value = extractMapLiteralValue(functionName, parameter, operands.get(i + 1));
            if (entries.putIfAbsent(key, value) != null) {
                throw QueryException.error(
                        "Duplicate entry in the MAP constructor in the call to function " + functionName + " - " +
                        "argument #" + parameter.getOrdinal() + " (" + parameter.getName() + ")");
            }
        }
        return entries;
    }

    private static String extractMapLiteralValue(
            SqlIdentifier functionName,
            FunctionParameter parameter,
            SqlNode node
    ) {
        if (SqlUtil.isLiteral(node)) {
            SqlLiteral literal = (SqlLiteral) node;
            Object value = literal.getValue();
            if (value instanceof NlsString) {
                return ((NlsString) value).getValue();
            }
        }
        throw QueryException.error(
                "All values in the MAP constructor of the call to function " + functionName + ", argument #"
                + parameter.getOrdinal() + " (" + parameter.getName() + ") must be VARCHAR literals. "
                + "Actual argument is: "
                + (SqlUtil.isLiteral(node) ? ((SqlLiteral) node).getTypeName() : node.getKind()));
    }
}
