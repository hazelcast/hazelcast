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

import com.hazelcast.jet.sql.impl.aggregate.function.ImposeOrderFunction;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.virtual.ViewTable;
import com.hazelcast.jet.sql.impl.parse.SqlCreateMapping;
import com.hazelcast.jet.sql.impl.parse.SqlDropView;
import com.hazelcast.jet.sql.impl.parse.SqlExplainStatement;
import com.hazelcast.jet.sql.impl.parse.SqlShowStatement;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.jet.sql.impl.validate.literal.LiteralUtils;
import com.hazelcast.jet.sql.impl.validate.param.AbstractParameterConverter;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeCoercion;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeFactory;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils;
import com.hazelcast.sql.impl.ParameterConverter;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.schema.IMapResolver;
import com.hazelcast.sql.impl.schema.Mapping;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.ResourceUtil;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlQualified;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.sql.validate.SqlValidatorImplBridge;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Static;
import org.apache.calcite.util.Util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.jet.sql.impl.connector.SqlConnectorUtil.getJetSqlConnector;
import static com.hazelcast.jet.sql.impl.validate.ValidatorResource.RESOURCE;
import static org.apache.calcite.sql.JoinType.FULL;

/**
 * Hazelcast-specific SQL validator.
 */
public class HazelcastSqlValidator extends SqlValidatorImplBridge {

    private static final String OBJECT_NOT_FOUND = ResourceUtil.key(Static.RESOURCE.objectNotFound(""));
    private static final String OBJECT_NOT_FOUND_WITHIN = ResourceUtil.key(Static.RESOURCE.objectNotFoundWithin("", ""));

    private static final Config CONFIG = Config.DEFAULT
            .withIdentifierExpansion(true)
            .withSqlConformance(HazelcastSqlConformance.INSTANCE)
            .withTypeCoercionFactory(HazelcastTypeCoercion::new);

    /**
     * Visitor to rewrite Calcite operators to Hazelcast operators.
     */
    private final HazelcastSqlOperatorTable.RewriteVisitor rewriteVisitor;

    /**
     * Parameter converter that will be passed to parameter metadata.
     */
    private final Map<Integer, ParameterConverter> parameterConverterMap = new HashMap<>();

    /**
     * Parameter positions.
     */
    private final Map<Integer, SqlParserPos> parameterPositionMap = new HashMap<>();

    /**
     * Parameter values.
     */
    private final List<Object> arguments;

    private final IMapResolver iMapResolver;

    public HazelcastSqlValidator(
            SqlValidatorCatalogReader catalogReader,
            List<Object> arguments,
            IMapResolver iMapResolver
    ) {
        super(HazelcastSqlOperatorTable.instance(), catalogReader, HazelcastTypeFactory.INSTANCE, CONFIG);

        this.rewriteVisitor = new HazelcastSqlOperatorTable.RewriteVisitor(this);
        this.arguments = arguments;
        this.iMapResolver = iMapResolver;
    }

    @Override
    public SqlNode validate(SqlNode topNode) {
        if (topNode instanceof SqlDropView) {
            return topNode;
        }

        if (topNode.getKind().belongsTo(SqlKind.DDL)) {
            topNode.validate(this, getEmptyScope());
            return topNode;
        }

        if (topNode instanceof SqlShowStatement) {
            return topNode;
        }

        if (topNode instanceof SqlExplainStatement) {
            /*
             * Just FYI, why do we do set validated explicandum back.
             *
             * There was a corner case with queries where ORDER BY is present.
             * SqlOrderBy is present as AST node (or SqlNode),
             * but then it becomes embedded as part of SqlSelect AST node,
             * and node itself is removed in performUnconditionalRewrites().
             * As a result, ORDER BY is absent as operator
             * on the next validation & optimization phases
             * and also doesn't present in SUPPORTED_KINDS.
             *
             * Explain query contains explicandum query, and
             * performUnconditionalRewrites() doesn't rewrite anything for EXPLAIN.
             * It's a reason why we do it (extraction, validation & re-setting) manually.
             */

            SqlExplainStatement explainStatement = (SqlExplainStatement) topNode;
            SqlNode explicandum = explainStatement.getExplicandum();
            explicandum = super.validate(explicandum);
            explainStatement.setExplicandum(explicandum);
            return explainStatement;
        }

        return super.validate(topNode);
    }

    @Override
    public void validateQuery(SqlNode node, SqlValidatorScope scope, RelDataType targetRowType) {
        super.validateQuery(node, scope, targetRowType);
        if (node instanceof SqlSelect) {
            validateSelect((SqlSelect) node, scope);
        }
    }

    private void validateSelect(SqlSelect select, SqlValidatorScope scope) {
        // Derive the types for offset-fetch expressions, Calcite doesn't do
        // that automatically.

        SqlNode fetch = select.getFetch();
        if (fetch != null) {
            deriveType(scope, fetch);
            fetch.validate(this, getEmptyScope());
        }

        SqlNode offset = select.getOffset();
        if (offset != null) {
            deriveType(scope, offset);
            offset.validate(this, getEmptyScope());
        }
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
        if (isHiddenColumn(exp, scope)) {
            return;
        }

        super.addToSelectList(list, aliases, fieldList, exp, scope, includeSystemVars);
    }

    @Override
    protected void validateFrom(SqlNode node, RelDataType targetRowType, SqlValidatorScope scope) {
        super.validateFrom(node, targetRowType, scope);

        if (countOrderingFunctions(node) > 1) {
            throw newValidationError(node, RESOURCE.multipleOrderingFunctionsNotSupported());
        }
    }

    private static int countOrderingFunctions(SqlNode node) {
        class OrderingFunctionCounter extends SqlBasicVisitor<Void> {
            int count;

            @Override
            public Void visit(SqlCall call) {
                SqlOperator operator = call.getOperator();
                if (operator instanceof ImposeOrderFunction) {
                    count++;
                }
                return super.visit(call);
            }
        }

        OrderingFunctionCounter counter = new OrderingFunctionCounter();
        node.accept(counter);
        return counter.count;
    }

    @Override
    protected void validateJoin(SqlJoin join, SqlValidatorScope scope) {
        super.validateJoin(join, scope);

        if (join.getJoinType() == FULL) {
            throw QueryException.error(SqlErrorCode.PARSING, "FULL join not supported");
        }
    }

    @Override
    protected SqlSelect createSourceSelectForUpdate(SqlUpdate update) {
        SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
        Table table = extractTable((SqlIdentifier) update.getTargetTable());
        if (table != null) {
            if (table instanceof ViewTable) {
                throw QueryException.error("DML operations not supported for views");
            }
            SqlConnector connector = getJetSqlConnector(table);

            // only tables with primary keys can be updated
            if (connector.getPrimaryKey(table).isEmpty()) {
                throw QueryException.error("Cannot UPDATE " + update.getTargetTable() + ": it doesn't have a primary key");
            }

            // add all fields, even hidden ones...
            table.getFields().forEach(field -> selectList.add(new SqlIdentifier(field.getName(), SqlParserPos.ZERO)));
        }
        int ordinal = 0;
        for (SqlNode exp : update.getSourceExpressionList()) {
            // Force unique aliases to avoid a duplicate for Y with
            // SET X=Y
            String alias = SqlUtil.deriveAliasFromOrdinal(ordinal);
            selectList.add(SqlValidatorUtil.addAlias(exp, alias));
            ++ordinal;
        }

        SqlNode sourceTable = update.getTargetTable();
        if (update.getAlias() != null) {
            sourceTable = SqlValidatorUtil.addAlias(sourceTable, update.getAlias().getSimple());
        }
        return new SqlSelect(SqlParserPos.ZERO, null, selectList, sourceTable,
                update.getCondition(), null, null, null, null, null, null, null);
    }

    @Override
    public void validateUpdate(SqlUpdate update) {
        super.validateUpdate(update);

        // hack around Calcite deficiency of not deriving types for fields in sourceExpressionList...
        // see HazelcastTypeCoercion.coerceSourceRowType()
        SqlNodeList selectList = update.getSourceSelect().getSelectList();
        SqlNodeList sourceExpressionList = update.getSourceExpressionList();
        for (int i = 0; i < sourceExpressionList.size(); i++) {
            update.getSourceExpressionList().set(i, selectList.get(selectList.size() - sourceExpressionList.size() + i));
        }

        // UPDATE FROM SELECT is transformed into join (which is not supported yet):
        // UPDATE m1 SET __key = m2.this FROM m2 WHERE m1.__key = m2.__key
        // UPDATE m1 SET __key = (SELECT this FROM m2) WHERE __key = 1
        // UPDATE m1 SET __key = (SELECT m2.this FROM m2 WHERE m1.__key = m2.__key)
        update.getSourceSelect().getSelectList().accept(new SqlBasicVisitor<Void>() {
            @Override
            public Void visit(SqlCall call) {
                if (call.getKind() == SqlKind.SELECT) {
                    throw newValidationError(update, RESOURCE.updateFromSelectNotSupported());
                }

                return call.getOperator().acceptCall(this, call);
            }
        });
    }

    @Override
    protected SqlSelect createSourceSelectForDelete(SqlDelete delete) {
        SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
        Table table = extractTable((SqlIdentifier) delete.getTargetTable());
        if (table != null) {
            if (table instanceof ViewTable) {
                throw QueryException.error("DML operations not supported for views");
            }
            SqlConnector connector = getJetSqlConnector(table);

            // We need to feed primary keys to the delete processor so that it can directly delete the records.
            // Therefore we use the primary key for the select list.
            connector.getPrimaryKey(table).forEach(name -> selectList.add(new SqlIdentifier(name, SqlParserPos.ZERO)));
            if (selectList.size() == 0) {
                throw QueryException.error("Cannot DELETE from " + delete.getTargetTable() + ": it doesn't have a primary key");
            }
        }

        SqlNode sourceTable = delete.getTargetTable();
        if (delete.getAlias() != null) {
            sourceTable = SqlValidatorUtil.addAlias(sourceTable, delete.getAlias().getSimple());
        }
        return new SqlSelect(SqlParserPos.ZERO, null, selectList, sourceTable,
                delete.getCondition(), null, null, null, null, null, null, null);
    }

    private Table extractTable(SqlIdentifier identifier) {
        SqlValidatorTable validatorTable = getCatalogReader().getTable(identifier.names);
        return validatorTable == null ? null : validatorTable.unwrap(HazelcastTable.class).getTarget();
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
                QueryDataType targetType = HazelcastTypeUtils.toHazelcastType(rowType.getFieldList().get(i).getType());
                converter = AbstractParameterConverter.from(targetType, i, parameterPositionMap.get(i));
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

    @Override
    public CalciteContextException newValidationError(SqlNode node, Resources.ExInst<SqlValidatorException> e) {
        assert node != null;

        CalciteContextException exception = SqlUtil.newContextException(node.getParserPosition(), e);
        if (OBJECT_NOT_FOUND.equals(ResourceUtil.key(e)) || OBJECT_NOT_FOUND_WITHIN.equals(ResourceUtil.key(e))) {
            Object[] arguments = ResourceUtil.args(e);
            String identifier = (arguments != null && arguments.length > 0) ? String.valueOf(arguments[0]) : null;
            Mapping mapping = identifier != null ? iMapResolver.resolve(identifier) : null;
            String sql = mapping != null ? SqlCreateMapping.unparse(mapping) : null;
            String message = sql != null ? ValidatorResource.imapNotMapped(e.str(), identifier, sql) : e.str();
            throw QueryException.error(SqlErrorCode.OBJECT_NOT_FOUND, message, exception, sql);
        }
        return exception;
    }
}
