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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.sql.impl.connector.map.IMapPlan.IMapDeletePlan;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.calcite.OptimizerContext;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.RexToExpressionVisitor;
import com.hazelcast.sql.impl.calcite.parse.QueryParseResult;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.optimizer.PlanKey;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import com.hazelcast.sql.impl.plan.node.PlanNodeFieldTypeProvider;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Litmus;

import java.util.Objects;

public final class IMapPlanner {

    private static final SqlIdentifier PRIMARY_KEY_IDENTIFIER =
            new SqlIdentifier(IMapSqlConnector.PRIMARY_KEY_LIST, SqlParserPos.ZERO);

    private final IMapPlanExecutor planExecutor;

    public IMapPlanner(HazelcastInstance hazelcastInstance) {
        this.planExecutor = new IMapPlanExecutor(hazelcastInstance);
    }

    public SqlPlan tryCreatePlan(PlanKey planKey, QueryParseResult parseResult, OptimizerContext context) {
        //noinspection SwitchStatementWithTooFewBranches
        switch (parseResult.getNode().getKind()) {
            case DELETE:
                return tryCreateDeletePlan(parseResult, context, planKey);
            default:
                return null;
        }
    }

    private SqlPlan tryCreateDeletePlan(QueryParseResult parseResult, OptimizerContext context, PlanKey planKey) {
        SqlValidator validator = parseResult.getValidator();
        SqlDelete delete = (SqlDelete) parseResult.getNode();
        Table table = parseResult.getValidator().extractTable((SqlIdentifier) delete.getTargetTable());
        SqlSelect select = delete.getSourceSelect();

        SqlNode sqlCondition = extractPrimaryKeyCondition(validator, table, select);
        if (sqlCondition != null) {
            RexNode rexCondition = context.convertExpression(new QueryParseResult(
                    sqlCondition,
                    parseResult.getParameterMetadata(),
                    parseResult.getValidator(),
                    parseResult.getSqlBackend(),
                    parseResult.isInfiniteRows()
            ));
            RexToExpressionVisitor visitor = new RexToExpressionVisitor(
                    PlanNodeFieldTypeProvider.FAILING_FIELD_TYPE_PROVIDER,
                    QueryParameterMetadata.EMPTY
            );
            Expression<?> condition = rexCondition.accept(visitor);

            return new IMapDeletePlan(
                    planKey,
                    table.getObjectKey(),
                    planExecutor,
                    ((PartitionedMapTable) table).getMapName(),
                    condition
            );
        } else {
            return null;
        }
    }

    private SqlNode extractPrimaryKeyCondition(SqlValidator validator, Table table, SqlSelect select) {
        if (!(table instanceof PartitionedMapTable)) {
            return null;
        }

        if (!select.hasWhere()) {
            return null;
        }
        SqlBasicCall condition = (SqlBasicCall) select.getWhere();

        if (condition.getKind() != SqlKind.EQUALS) {
            return null;
        }

        SqlIdentifier primaryKeyIdentifier = validator.getSelectScope(select).fullyQualify(PRIMARY_KEY_IDENTIFIER).identifier;
        SqlNode primaryKeyCondition = extractPrimaryKeyCondition(condition, primaryKeyIdentifier, 0);
        return primaryKeyCondition != null ? primaryKeyCondition : extractPrimaryKeyCondition(condition, primaryKeyIdentifier, 1);
    }

    private SqlNode extractPrimaryKeyCondition(SqlBasicCall condition, SqlIdentifier primaryKeyIdentifier, int keyIndex) {
        SqlNode operand0 = condition.operand(keyIndex);
        if (operand0.equalsDeep(primaryKeyIdentifier, Litmus.IGNORE)) {
            SqlNode operand1 = condition.operand(1 - keyIndex);
            return operand1.accept(ConstantExpressionVisitor.INSTANCE) ? operand1 : null;
        }
        return null;
    }

    private static final class ConstantExpressionVisitor implements SqlVisitor<Boolean> {

        private static final ConstantExpressionVisitor INSTANCE = new ConstantExpressionVisitor();

        @Override
        public Boolean visit(SqlLiteral literal) {
            return true;
        }

        @Override
        public Boolean visit(SqlCall call) {
            return call.getOperandList().stream()
                    .filter(Objects::nonNull)
                    .allMatch(operand -> operand.accept(this));
        }

        @Override
        public Boolean visit(SqlNodeList nodeList) {
            return false;
        }

        @Override
        public Boolean visit(SqlIdentifier id) {
            return false;
        }

        @Override
        public Boolean visit(SqlDataTypeSpec type) {
            return true;
        }

        @Override
        public Boolean visit(SqlDynamicParam param) {
            return false;
        }

        @Override
        public Boolean visit(SqlIntervalQualifier intervalQualifier) {
            return false;
        }
    }
}
