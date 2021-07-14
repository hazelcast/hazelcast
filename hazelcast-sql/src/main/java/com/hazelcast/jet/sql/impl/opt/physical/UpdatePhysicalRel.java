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

import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toMap;
import static org.apache.calcite.rel.core.TableModify.Operation.UPDATE;

public class UpdatePhysicalRel extends TableModify implements PhysicalRel {

    UpdatePhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            Prepare.CatalogReader catalogReader,
            RelNode input,
            List<String> updateColumnList,
            List<RexNode> sourceExpressionList,
            boolean flattened
    ) {
        super(cluster, traitSet, table, catalogReader, input, UPDATE, updateColumnList, sourceExpressionList, flattened);
    }

    public Map<String, Expression<?>> updates(QueryParameterMetadata parameterMetadata) {
        List<Expression<?>> projects = project(OptUtils.schema(getTable()), getSourceExpressionList(), parameterMetadata);
        return IntStream.range(0, projects.size())
                .boxed()
                .collect(toMap(i -> getUpdateColumnList().get(i), projects::get));
    }

    @Override
    public PlanNodeSchema schema(QueryParameterMetadata parameterMetadata) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Vertex accept(CreateDagVisitor visitor) {
        return visitor.onUpdate(this);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new UpdatePhysicalRel(
                getCluster(),
                traitSet,
                getTable(),
                getCatalogReader(),
                sole(inputs),
                getUpdateColumnList(),
                getSourceExpressionList(),
                isFlattened()
        );
    }
}
