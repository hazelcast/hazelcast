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
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.RexToExpressionVisitor;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.plan.node.PlanNodeFieldTypeProvider;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.List;

public class DeleteByKeyMapPhysicalRel extends AbstractRelNode implements PhysicalRel {

    private final PartitionedMapTable table;
    private final RexNode primaryKeyCondition;

    DeleteByKeyMapPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            PartitionedMapTable table,
            RexNode primaryKeyCondition
    ) {
        super(cluster, traitSet);

        this.table = table;
        this.primaryKeyCondition = primaryKeyCondition;
    }

    public String mapName() {
        return table.getMapName();
    }

    public PlanObjectKey objectKey() {
        return table.getObjectKey();
    }

    public Expression<?> primaryKeyCondition(QueryParameterMetadata parameterMetadata) {
        RexToExpressionVisitor visitor = new RexToExpressionVisitor(
                PlanNodeFieldTypeProvider.FAILING_FIELD_TYPE_PROVIDER,
                parameterMetadata
        );
        return primaryKeyCondition.accept(visitor);
    }

    @Override
    public PlanNodeSchema schema(QueryParameterMetadata parameterMetadata) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Vertex accept(CreateDagVisitor visitor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelDataType deriveRowType() {
        return RelOptUtil.createDmlRowType(SqlKind.DELETE, getCluster().getTypeFactory());
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return pw
                .item("table", table.getSqlName())
                .item("primaryKeyCondition", primaryKeyCondition);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new DeleteByKeyMapPhysicalRel(getCluster(), traitSet, table, primaryKeyCondition);
    }
}
