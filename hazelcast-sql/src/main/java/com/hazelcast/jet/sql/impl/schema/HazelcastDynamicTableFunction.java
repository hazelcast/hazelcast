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
import com.hazelcast.jet.sql.impl.validate.HazelcastSqlValidator;
import com.hazelcast.jet.sql.impl.validate.ValidationUtil;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.Table;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypePrecedenceList;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.calcite.sql.type.SqlTypeName.MAP;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

/**
 * A table function return type of which is NOT known upfront and is determined during validation phase.
 */
public abstract class HazelcastDynamicTableFunction extends HazelcastTableSourceFunction {

    protected HazelcastDynamicTableFunction(
            String name,
            HazelcastSqlOperandMetadata operandMetadata,
            Function<List<Object>, Table> tableFn,
            SqlConnector connector
    ) {
        super(
                name,
                operandMetadata,
                binding -> inferReturnType(name, operandMetadata.parameters(), tableFn, binding),
                connector
        );

        assert operandMetadata.parameters().stream()
                .map(HazelcastTableFunctionParameter::type)
                .allMatch(type -> type == VARCHAR || type == MAP);
    }

    public final HazelcastTable toTable(RelDataType rowType) {
        return ((HazelcastFunctionRelDataType) rowType).table();
    }

    private static RelDataType inferReturnType(
            String name,
            List<HazelcastTableFunctionParameter> parameters,
            Function<List<Object>, Table> tableFn,
            SqlOperatorBinding callBinding
    ) {
        List<Object> arguments = toArguments(name, parameters, callBinding);
        HazelcastTable table = new HazelcastTable(tableFn.apply(arguments), UnknownStatistic.INSTANCE);
        RelDataType rowType = table.getRowType(callBinding.getTypeFactory());
        return new HazelcastFunctionRelDataType(table, rowType);
    }

    private static List<Object> toArguments(
            String functionName,
            List<HazelcastTableFunctionParameter> parameters,
            SqlOperatorBinding callBinding
    ) {
        SqlCallBinding binding = (SqlCallBinding) callBinding;
        SqlCall call = binding.getCall();
        HazelcastSqlValidator validator = (HazelcastSqlValidator) binding.getValidator();

        return ValidationUtil.hasAssignment(call)
                ? fromNamedArguments(functionName, parameters, call, validator)
                : fromPositionalArguments(functionName, parameters, call, validator);
    }

    private static List<Object> fromNamedArguments(
            String functionName,
            List<HazelcastTableFunctionParameter> parameters,
            SqlCall call,
            HazelcastSqlValidator validator
    ) {
        List<Object> arguments = new ArrayList<>(parameters.size());
        for (HazelcastTableFunctionParameter parameter : parameters) {
            SqlNode operand = findOperandByName(parameter.name(), call);
            Object value = operand == null ? null : extractValue(functionName, parameter, operand, validator);
            arguments.add(value);
        }
        return arguments;
    }

    private static SqlNode findOperandByName(String name, SqlCall call) {
        for (int i = 0; i < call.operandCount(); i++) {
            SqlCall assignment = call.operand(i);
            SqlIdentifier id = assignment.operand(1);
            if (name.equals(id.getSimple())) {
                return assignment.operand(0);
            }
        }
        return null;
    }

    private static List<Object> fromPositionalArguments(
            String functionName,
            List<HazelcastTableFunctionParameter> parameters,
            SqlCall call,
            HazelcastSqlValidator validator
    ) {
        List<Object> arguments = new ArrayList<>(parameters.size());
        for (int i = 0; i < call.operandCount(); i++) {
            Object value = extractValue(functionName, parameters.get(i), call.operand(i), validator);
            arguments.add(value);
        }
        for (int i = call.operandCount(); i < parameters.size(); i++) {
            arguments.add(null);
        }
        return arguments;
    }

    private static Object extractValue(
            String functionName,
            HazelcastTableFunctionParameter parameter,
            SqlNode operand,
            HazelcastSqlValidator validator
    ) {
        if (operand.getKind() == SqlKind.DEFAULT) {
            return null;
        }
        if (SqlUtil.isNullLiteral(operand, true)) {
            return null;
        }
        if (operand.getKind() == SqlKind.DYNAMIC_PARAM) {
            return validator.getArgumentAt(((SqlDynamicParam) operand).getIndex());
        }

        SqlTypeName parameterType = parameter.type();
        if (SqlUtil.isLiteral(operand) && parameterType == SqlTypeName.VARCHAR) {
            String value = extractStringValue(((SqlLiteral) operand));
            if (value != null) {
                return value;
            }
        } else if (operand.getKind() == SqlKind.MAP_VALUE_CONSTRUCTOR && parameterType == SqlTypeName.MAP) {
            return extractMapValue(functionName, parameter, (SqlCall) operand, validator);
        }
        throw QueryException.error("Invalid argument of a call to function " + functionName + " - #"
                + parameter.ordinal() + " (" + parameter.name() + "). Expected: " + parameterType
                + ", actual: "
                + (SqlUtil.isLiteral(operand) ? ((SqlLiteral) operand).getTypeName() : operand.getKind()));
    }

    private static String extractStringValue(SqlLiteral literal) {
        Object value = literal.getValue();
        return value instanceof NlsString ? ((NlsString) value).getValue() : null;
    }

    private static Map<String, String> extractMapValue(
            String functionName,
            HazelcastTableFunctionParameter parameter,
            SqlCall call,
            HazelcastSqlValidator validator
    ) {
        List<SqlNode> operands = call.getOperandList();
        Map<String, String> entries = new HashMap<>();
        for (int i = 0; i < operands.size(); i += 2) {
            String key = extractMapStringValue(functionName, parameter, operands.get(i), validator);
            String value = extractMapStringValue(functionName, parameter, operands.get(i + 1), validator);
            if (entries.putIfAbsent(key, value) != null) {
                throw QueryException.error(
                        "Duplicate entry in the MAP constructor in the call to function " + functionName + " - " +
                                "argument #" + parameter.ordinal() + " (" + parameter.name() + ")");
            }
        }
        return entries;
    }

    private static String extractMapStringValue(
            String functionName,
            HazelcastTableFunctionParameter parameter,
            SqlNode node,
            HazelcastSqlValidator validator
    ) {
        if (node.getKind() == SqlKind.DYNAMIC_PARAM) {
            Object value = validator.getArgumentAt(((SqlDynamicParam) node).getIndex());
            if (value instanceof String) {
                return (String) value;
            }
        }
        if (SqlUtil.isLiteral(node)) {
            SqlLiteral literal = (SqlLiteral) node;
            Object value = literal.getValue();
            if (value instanceof NlsString) {
                return ((NlsString) value).getValue();
            }
        }
        throw QueryException.error(
                "All values in the MAP constructor of the call to function " + functionName + ", argument #"
                        + parameter.ordinal() + " (" + parameter.name() + ") must be VARCHAR literals. "
                        + "Actual argument is: "
                        + (SqlUtil.isLiteral(node) ? ((SqlLiteral) node).getTypeName() : node.getKind()));
    }


    /**
     * The only purpose of this class is to be able to pass the {@code
     * HazelcastTable} object to place where the function is used.
     */
    private static final class HazelcastFunctionRelDataType implements RelDataType {

        private final HazelcastTable table;
        private final RelDataType delegate;

        private HazelcastFunctionRelDataType(HazelcastTable table, RelDataType delegate) {
            this.delegate = delegate;
            this.table = table;
        }

        private HazelcastTable table() {
            return table;
        }

        @Override
        public boolean isStruct() {
            return delegate.isStruct();
        }

        @Override
        public List<RelDataTypeField> getFieldList() {
            return delegate.getFieldList();
        }

        @Override
        public List<String> getFieldNames() {
            return delegate.getFieldNames();
        }

        @Override
        public int getFieldCount() {
            return delegate.getFieldCount();
        }

        @Override
        public StructKind getStructKind() {
            return delegate.getStructKind();
        }

        @Override
        public RelDataTypeField getField(String fieldName, boolean caseSensitive, boolean elideRecord) {
            return delegate.getField(fieldName, caseSensitive, elideRecord);
        }

        @Override
        public boolean isNullable() {
            return delegate.isNullable();
        }

        @Override
        public RelDataType getComponentType() {
            return delegate.getComponentType();
        }

        @Override
        public RelDataType getKeyType() {
            return delegate.getKeyType();
        }

        @Override
        public RelDataType getValueType() {
            return delegate.getValueType();
        }

        @Override
        public Charset getCharset() {
            return delegate.getCharset();
        }

        @Override
        public SqlCollation getCollation() {
            return delegate.getCollation();
        }

        @Override
        public SqlIntervalQualifier getIntervalQualifier() {
            return delegate.getIntervalQualifier();
        }

        @Override
        public int getPrecision() {
            return delegate.getPrecision();
        }

        @Override
        public int getScale() {
            return delegate.getScale();
        }

        @Override
        public SqlTypeName getSqlTypeName() {
            return delegate.getSqlTypeName();
        }

        @Override
        public SqlIdentifier getSqlIdentifier() {
            return delegate.getSqlIdentifier();
        }

        @Override
        public String toString() {
            return delegate.toString();
        }

        @Override
        public String getFullTypeString() {
            return delegate.getFullTypeString();
        }

        @Override
        public RelDataTypeFamily getFamily() {
            return delegate.getFamily();
        }

        @Override
        public RelDataTypePrecedenceList getPrecedenceList() {
            return delegate.getPrecedenceList();
        }

        @Override
        public RelDataTypeComparability getComparability() {
            return delegate.getComparability();
        }

        @Override
        public boolean isDynamicStruct() {
            return delegate.isDynamicStruct();
        }
    }
}
