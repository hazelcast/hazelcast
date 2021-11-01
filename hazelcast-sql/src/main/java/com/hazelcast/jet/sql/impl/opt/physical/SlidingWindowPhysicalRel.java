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

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.SlidingWindowPolicy;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.aggregate.WindowUtils;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.physical.visitor.RexToExpressionVisitor;
import com.hazelcast.jet.sql.impl.validate.HazelcastSqlOperatorTable;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.plan.HazelcastRelOptCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;

import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.jet.core.SlidingWindowPolicy.tumblingWinPolicy;
import static com.hazelcast.sql.impl.plan.node.PlanNodeFieldTypeProvider.FAILING_FIELD_TYPE_PROVIDER;

public class SlidingWindowPhysicalRel extends TableFunctionScan implements PhysicalRel {

    SlidingWindowPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelNode> inputs,
            RexNode rexCall,
            Type elementType,
            RelDataType rowType,
            Set<RelColumnMapping> columnMappings
    ) {
        super(cluster, traitSet, inputs, rexCall, elementType, rowType, columnMappings);

        SqlOperator operator = operator();
        checkTrue(operator == HazelcastSqlOperatorTable.TUMBLE, "Unsupported operator: " + operator);
    }

    public int orderingFieldIndex() {
        return ((RexInputRef) ((RexCall) operand(1)).getOperands().get(0)).getIndex();
    }

    public FunctionEx<ExpressionEvalContext, SlidingWindowPolicy> windowPolicyProvider() {
        QueryParameterMetadata parameterMetadata = ((HazelcastRelOptCluster) getCluster()).getParameterMetadata();
        RexToExpressionVisitor visitor = new RexToExpressionVisitor(FAILING_FIELD_TYPE_PROVIDER, parameterMetadata);
        if (operator() == HazelcastSqlOperatorTable.TUMBLE) {
            Expression<?> windowSizeExpression = operand(2).accept(visitor);
            return context -> tumblingWinPolicy(WindowUtils.extractMillis(windowSizeExpression, context));
        } else {
            throw new IllegalArgumentException();
        }
    }

    private SqlOperator operator() {
        return ((RexCall) getCall()).getOperator();
    }

    private RexNode operand(int index) {
        return ((RexCall) getCall()).getOperands().get(index);
    }

    public RelNode getInput() {
        return sole(getInputs());
    }

    @Override
    public PlanNodeSchema schema(QueryParameterMetadata parameterMetadata) {
        return OptUtils.schema(getRowType());
    }

    @Override
    public Vertex accept(CreateDagVisitor visitor) {
        return visitor.onSlidingWindow(this);
    }

    @Override
    public TableFunctionScan copy(
            RelTraitSet traitSet,
            List<RelNode> inputs,
            RexNode rexCall,
            Type elementType,
            RelDataType rowType,
            Set<RelColumnMapping> columnMappings
    ) {
        return new SlidingWindowPhysicalRel(getCluster(), traitSet, inputs, rexCall, elementType, rowType, columnMappings);
    }
}
