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

package com.hazelcast.sql.impl.calcite.validate;

import com.hazelcast.sql.impl.ParameterConverter;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.calcite.validate.literal.LiteralUtils;
import com.hazelcast.sql.impl.calcite.validate.param.StrictParameterConverter;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeCoercion;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlQualified;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImplBridge;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.sql.impl.calcite.parse.UnsupportedOperationVisitor.error;

/**
 * Hazelcast-specific SQL validator.
 */
public class HazelcastSqlValidator extends SqlValidatorImplBridge {

    private static final Config CONFIG = Config.DEFAULT.withIdentifierExpansion(true);

    protected boolean isUpdate;

    /** Visitor to rewrite Calcite operators to Hazelcast operators. */
    private final HazelcastSqlOperatorTable.RewriteVisitor rewriteVisitor;

    /** Parameter converter that will be passed to parameter metadata. */
    private final Map<Integer, ParameterConverter> parameterConverterMap = new HashMap<>();

    /** Parameter positions. */
    private final Map<Integer, SqlParserPos> parameterPositionMap = new HashMap<>();

    /** Parameter values. */
    private final List<Object> arguments;

    public HazelcastSqlValidator(
            SqlValidatorCatalogReader catalogReader,
            HazelcastTypeFactory typeFactory,
            SqlConformance conformance,
            List<Object> arguments
    ) {
        this(null, catalogReader, typeFactory, conformance, arguments);
    }

    public HazelcastSqlValidator(
            SqlOperatorTable extensionOperatorTable,
            SqlValidatorCatalogReader catalogReader,
            HazelcastTypeFactory typeFactory,
            SqlConformance conformance,
            List<Object> arguments
    ) {
        super(operatorTable(extensionOperatorTable), catalogReader, typeFactory, CONFIG.withSqlConformance(conformance));

        setTypeCoercion(new HazelcastTypeCoercion(this));

        this.rewriteVisitor = new HazelcastSqlOperatorTable.RewriteVisitor(this);
        this.arguments = arguments;
    }

    private static SqlOperatorTable operatorTable(SqlOperatorTable extensionOperatorTable) {
        List<SqlOperatorTable> operatorTables = new ArrayList<>();

        if (extensionOperatorTable != null) {
            operatorTables.add(extensionOperatorTable);
        }

        operatorTables.add(HazelcastSqlOperatorTable.instance());

        return new ChainedSqlOperatorTable(operatorTables);
    }

    @Override
    protected void addToSelectList(
            List<SqlNode> list,
            Set<String> aliases,
            List<Map.Entry<String, RelDataType>> fieldList,
            SqlNode exp,
            SelectScope scope,
            boolean includeSystemVars
    ) {
        if (isHiddenColumn(exp, scope) && !isUpdate) {
            return;
        }

        super.addToSelectList(list, aliases, fieldList, exp, scope, includeSystemVars);
    }

    @Override
    public RelDataType deriveTypeImpl(SqlValidatorScope scope, SqlNode operand) {
        if (operand.getKind() == SqlKind.LITERAL) {
            RelDataType literalType = LiteralUtils.literalType(operand, (HazelcastTypeFactory) typeFactory);

            if (literalType != null) {
                return literalType;
            }
        }

        return super.deriveTypeImpl(scope, operand);
    }

    @Override
    public void validateLiteral(SqlLiteral literal) {
        if (literal instanceof SqlIntervalLiteral) {
            super.validateLiteral(literal);
        }

        // Disable validation of other literals
    }

    @Override
    public void validateDynamicParam(SqlDynamicParam dynamicParam) {
        parameterPositionMap.put(dynamicParam.getIndex(), dynamicParam.getParserPosition());
    }

    @Override
    public void validateCall(SqlCall call, SqlValidatorScope scope) {
        // Enforce type derivation for all calls before validation. Calcite may
        // skip it if a call has a fixed type, for instance AND always has
        // BOOLEAN type, so operands may end up having no validated type.
        deriveType(scope, call);
        super.validateCall(call, scope);
    }

    @Override
    public void validateQuery(SqlNode node, SqlValidatorScope scope, RelDataType targetRowType) {
        super.validateQuery(node, scope, targetRowType);

        if (node instanceof SqlSelect) {
            validateSelect((SqlSelect) node, scope);
        }
    }

    protected void validateSelect(SqlSelect select, SqlValidatorScope scope) {
        // Derive the types for offset-fetch expressions, Calcite doesn't do
        // that automatically.

        SqlNode offset = select.getOffset();
        if (offset != null) {
            deriveType(scope, offset);
            validateNonNegativeValue(offset);
        }

        SqlNode fetch = select.getFetch();
        if (fetch != null) {
            deriveType(scope, fetch);
            validateNonNegativeValue(fetch);
        }
    }

    private void validateNonNegativeValue(SqlNode sqlNode) {
        if (!(sqlNode instanceof SqlNumericLiteral)) {
            throw error(sqlNode, "FETCH/OFFSET must be a numeric literal");
        }
        Object value = ((SqlNumericLiteral) sqlNode).getValue();
        long value0 = ((Number) value).longValue();

        if (value0 < 0L) {
            throw error(sqlNode, "FETCH/OFFSET value cannot be negative: " + value0);
        }
    }

    @Override
    protected SqlNode performUnconditionalRewrites(SqlNode node, boolean underFrom) {
        SqlNode rewritten = super.performUnconditionalRewrites(node, underFrom);

        if (rewritten != null && rewritten.isA(SqlKind.TOP_LEVEL)) {
            // Rewrite operators to Hazelcast ones starting at every top node.
            // For instance, SELECT a + b is rewritten to SELECT a + b, where
            // the first '+' refers to the standard Calcite SqlStdOperatorTable.PLUS
            // operator and the second '+' refers to HazelcastSqlOperatorTable.PLUS
            // operator.
            rewritten.accept(rewriteVisitor);
        }

        return rewritten;
    }

    @Override
    public HazelcastTypeCoercion getTypeCoercion() {
        return (HazelcastTypeCoercion) super.getTypeCoercion();
    }

    public void setParameterConverter(int ordinal, ParameterConverter parameterConverter) {
        parameterConverterMap.put(ordinal, parameterConverter);
    }

    public Object getArgumentAt(int index) {
        ParameterConverter parameterConverter = parameterConverterMap.get(index);
        Object argument = arguments.get(index);
        return parameterConverter.convert(argument);
    }

    public ParameterConverter[] getParameterConverters(SqlNode node) {
        // Get original parameter row type.
        RelDataType rowType = getParameterRowType(node);

        // Create precedence-based converters with optional override by a more specialized converters.
        ParameterConverter[] res = new ParameterConverter[rowType.getFieldCount()];

        for (int i = 0; i < res.length; i++) {
            ParameterConverter converter = parameterConverterMap.get(i);

            if (converter == null) {
                converter = new StrictParameterConverter(
                        i,
                        parameterPositionMap.get(i),
                        HazelcastTypeUtils.toHazelcastType(rowType.getFieldList().get(i).getType().getSqlTypeName())
                );
            }

            res[i] = converter;
        }

        return res;
    }

    private boolean isHiddenColumn(SqlNode node, SelectScope scope) {
        if (!(node instanceof SqlIdentifier)) {
            return false;
        }

        SqlIdentifier identifier = (SqlIdentifier) node;

        String fieldName = extractFieldName(identifier, scope);

        if (fieldName == null) {
            return false;
        }

        SqlValidatorTable table = scope.fullyQualify(identifier).namespace.getTable();

        if (table == null) {
            return false;
        }

        HazelcastTable unwrappedTable = table.unwrap(HazelcastTable.class);

        if (unwrappedTable == null) {
            return false;
        }

        return unwrappedTable.isHidden(fieldName);
    }

    private String extractFieldName(SqlIdentifier identifier, SelectScope scope) {
        SqlCall call = makeNullaryCall(identifier);

        if (call != null) {
            return null;
        }

        SqlQualified qualified = scope.fullyQualify(identifier);

        List<String> names = qualified.identifier.names;

        if (names.size() < 2) {
            return null;
        }

        return Util.last(names);
    }

    /**
     * Returns whether the validated node returns an infinite number of rows.
     *
     * @throws IllegalStateException if called before the node is validated.
     */
    public boolean isInfiniteRows() {
        return false;
    }
}
