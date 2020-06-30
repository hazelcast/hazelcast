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

package com.hazelcast.sql.impl.calcite.validate;

import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastIntegerType;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeCoercion;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlQualified;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.util.Util;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.sql.impl.calcite.validate.SqlNodeUtil.isLiteral;
import static com.hazelcast.sql.impl.calcite.validate.SqlNodeUtil.isParameter;
import static com.hazelcast.sql.impl.calcite.validate.SqlNodeUtil.numericValue;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.canRepresent;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.isInteger;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.isNumeric;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.narrowestTypeFor;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.typeName;
import static org.apache.calcite.sql.type.SqlTypeName.CHAR;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.NULL;
import static org.apache.calcite.sql.type.SqlTypeName.NUMERIC_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Hazelcast-specific SQL validator.
 */
public class HazelcastSqlValidator extends SqlValidatorImpl {

    private static final Config CONFIG = Config.DEFAULT.withIdentifierExpansion(true);

    private final Map<SqlNode, RelDataType> knownNodeTypes = new HashMap<>();

    public HazelcastSqlValidator(SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader, RelDataTypeFactory typeFactory,
                                 SqlConformance conformance) {
        super(opTab, catalogReader, typeFactory, CONFIG.withSqlConformance(conformance));
        assert typeFactory instanceof HazelcastTypeFactory;
        setTypeCoercion(new HazelcastTypeCoercion(this));
    }

    /**
     * Sets a known type of the given node to the given type in this validator.
     *
     * @param node the node to set the known type of.
     * @param type the type to set the know node type to.
     */
    public void setKnownNodeType(SqlNode node, RelDataType type) {
        assert !getUnknownType().equals(type);
        knownNodeTypes.put(node, type);
    }

    /**
     * Obtains a type known by this validator for the given node.
     *
     * @param node the node to obtain the type of.
     * @return the node type known by this validator or {@code null} if the type
     * of the given node is not known yet.
     */
    public RelDataType getKnownNodeType(SqlNode node) {
        return knownNodeTypes.get(node);
    }

    @Override
    public void validateQuery(SqlNode node, SqlValidatorScope scope, RelDataType targetRowType) {
        super.validateQuery(node, scope, targetRowType);

        if (node instanceof SqlSelect) {
            // Derive the types for offset-fetch expressions, Calcite doesn't do
            // that automatically.

            SqlSelect select = (SqlSelect) node;

            SqlNode offset = select.getOffset();
            if (offset != null) {
                deriveType(scope, offset);
            }

            SqlNode fetch = select.getFetch();
            if (fetch != null) {
                deriveType(scope, fetch);
            }
        }
    }

    @Override
    public SqlNode validate(SqlNode topNode) {
        if (topNode.getKind().belongsTo(SqlKind.DDL)) {
            return topNode;
        }

        return super.validate(topNode);
    }

    @Override
    protected void addToSelectList(List<SqlNode> list, Set<String> aliases, List<Map.Entry<String, RelDataType>> fieldList,
                                   SqlNode exp, SelectScope scope, boolean includeSystemVars) {
        if (isHiddenColumn(exp, scope)) {
            return;
        }

        super.addToSelectList(list, aliases, fieldList, exp, scope, includeSystemVars);
    }

    @Override
    public RelDataType deriveType(SqlValidatorScope scope, SqlNode expression) {
        RelDataType derived = super.deriveType(scope, expression);
        assert derived != null;

        if (derived.getSqlTypeName() == CHAR) {
            // normalize CHAR to VARCHAR
            derived = HazelcastTypeFactory.INSTANCE.createSqlType(VARCHAR, derived.isNullable());
            setValidatedNodeType(expression, derived);
        }

        switch (expression.getKind()) {
            case LITERAL:
                return deriveLiteralType(derived, expression);

            case CAST:
                return deriveCastType(derived, scope, expression);

            default:
                return derived;
        }
    }

    @Override
    public void validateLiteral(SqlLiteral literal) {
        validateLiteral(literal, getValidatedNodeType(literal));
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
    protected SqlNode performUnconditionalRewrites(SqlNode node, boolean underFrom) {
        SqlNode rewritten = super.performUnconditionalRewrites(node, underFrom);

        if (rewritten != null && rewritten.isA(SqlKind.TOP_LEVEL)) {
            // rewrite operators to Hazelcast ones starting at every top node
            rewritten.accept(HazelcastOperatorTableVisitor.INSTANCE);
        }

        return rewritten;
    }

    private RelDataType deriveLiteralType(RelDataType derived, SqlNode expression) {
        RelDataType known = knownNodeTypes.get(expression);
        if (derived == known) {
            return derived;
        }

        SqlLiteral literal = (SqlLiteral) expression;

        if (HazelcastIntegerType.supports(typeName(derived)) && literal.getValue() != null) {
            // Assign narrowest type to non-null integer literals.

            derived = HazelcastIntegerType.deriveLiteralType(literal);
            setKnownAndValidatedNodeType(expression, derived);
        } else if (typeName(derived) == DECIMAL) {
            // Assign DOUBLE type to any standalone floating point literal: the
            // exact type is inferred later from the context in which the literal
            // appears.

            derived = HazelcastTypeFactory.INSTANCE.createSqlType(DOUBLE);
            setKnownAndValidatedNodeType(expression, derived);
        }

        return derived;
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private RelDataType deriveCastType(RelDataType derived, SqlValidatorScope scope, SqlNode expression) {
        RelDataType known = knownNodeTypes.get(expression);
        if (derived == known) {
            return derived;
        }

        SqlCall call = (SqlCall) expression;
        SqlNode operand = call.operand(0);
        RelDataType from = deriveType(scope, operand);

        RelDataType to = deriveType(scope, call.operand(1));
        assert !to.isNullable();

        // Handle NULL.

        if (SqlUtil.isNullLiteral(operand, false)) {
            setKnownAndValidatedNodeType(operand, HazelcastTypeFactory.INSTANCE.createSqlType(NULL));
            derived = HazelcastTypeFactory.INSTANCE.createTypeWithNullability(to, true);
            setKnownAndValidatedNodeType(expression, derived);
            return derived;
        }

        derived = to;

        // Assign type for parameters.

        if (isParameter(operand)) {
            from = HazelcastTypeFactory.INSTANCE.createTypeWithNullability(to, true);
        }

        // Assign type to numeric literals and validate them.

        BigDecimal numeric = isNumeric(from) || isNumeric(to) ? numericValue(operand) : null;

        if (numeric != null) {
            from = narrowestTypeFor(numeric, typeName(to));
        }

        if (isLiteral(operand)) {
            validateLiteral((SqlLiteral) operand, to);
        }

        // Infer return type.

        if (isInteger(to) && isInteger(from)) {
            derived = HazelcastIntegerType.deriveCastType(from, to);
        } else if (isInteger(to) && numeric != null) {
            long longValue = numeric.longValue();
            derived = HazelcastIntegerType.deriveCastType(longValue, to);
        }

        derived = HazelcastTypeFactory.INSTANCE.createTypeWithNullability(derived, from.isNullable());

        setKnownAndValidatedNodeType(operand, from);
        setKnownAndValidatedNodeType(expression, derived);

        return derived;
    }

    private void validateLiteral(SqlLiteral literal, RelDataType type) {
        SqlTypeName literalTypeName = literal.getTypeName();

        if (!canRepresent(literal, type)) {
            if (NUMERIC_TYPES.contains(literalTypeName) && isNumeric(type)) {
                throw newValidationError(literal, RESOURCE.numberLiteralOutOfRange(literal.toString()));
            } else {
                throw SqlUtil.newContextException(literal.getParserPosition(),
                        RESOURCE.invalidLiteral(literal.toString(), type.toString()));
            }
        }

        if (literalTypeName != DECIMAL) {
            super.validateLiteral(literal);
        }
    }

    private void setKnownAndValidatedNodeType(SqlNode node, RelDataType type) {
        setKnownNodeType(node, type);
        setValidatedNodeType(node, type);
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

}
