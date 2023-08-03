/*
 * Copyright 2023 Hazelcast Inc.
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

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.impl.connector.HazelcastRexNode;
import com.hazelcast.jet.sql.impl.connector.SqlConnector.DagBuildContext;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeFieldTypeProvider;
import com.hazelcast.sql.impl.schema.Table;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class DagBuildContextImpl implements DagBuildContext {
    private final NodeEngine nodeEngine;
    private final DAG dag;
    private final QueryParameterMetadata parameterMetadata;
    private Table table;
    private PhysicalRel rel;

    public DagBuildContextImpl(NodeEngine nodeEngine, DAG dag, QueryParameterMetadata parameterMetadata) {
        this.nodeEngine = requireNonNull(nodeEngine);
        this.dag = requireNonNull(dag);
        this.parameterMetadata = parameterMetadata;
    }

    @Nonnull
    @Override
    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    @Nonnull
    @Override
    public DAG getDag() {
        return dag;
    }

    @Nonnull
    @Override
    public Table getTable() {
        if (table == null) {
            throw new IllegalStateException("table not available");
        }
        return table;
    }

    public PhysicalRel getRel() {
        return rel;
    }

    public void setRel(@Nullable PhysicalRel rel) {
        this.rel = rel;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    @Nullable
    @SuppressWarnings("unchecked")
    @Override
    public Expression<Boolean> convertFilter(@Nullable HazelcastRexNode node) {
        if (node == null) {
            return null;
        }
        return (Expression<Boolean>) node.unwrap(RexNode.class).accept(createVisitor());
    }

    @Nonnull
    @Override
    public List<Expression<?>> convertProjection(@Nonnull List<HazelcastRexNode> nodes) {
        RexVisitor<Expression<?>> visitor = createVisitor();
        return Util.toList(nodes, node -> node.unwrap(RexNode.class).accept(visitor));
    }

    @Nonnull
    private RexVisitor<Expression<?>> createVisitor() {
        PlanNodeFieldTypeProvider schema;
        if (table != null) {
            schema = OptUtils.schema(table);
        } else if (rel.getInputs().size() != 1) {
            schema = PlanNodeFieldTypeProvider.FAILING_FIELD_TYPE_PROVIDER;
        } else {
            schema = ((PhysicalRel) rel.getInput(0)).schema(parameterMetadata);
        }
        return OptUtils.createRexToExpressionVisitor(schema, parameterMetadata);
    }

    public QueryParameterMetadata getParameterMetadata() {
        return parameterMetadata;
    }
}
