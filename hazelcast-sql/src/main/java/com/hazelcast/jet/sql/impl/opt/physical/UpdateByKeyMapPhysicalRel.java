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
import com.hazelcast.jet.sql.impl.connector.map.UpdatingEntryProcessor;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.jet.sql.impl.opt.physical.visitor.RexToExpressionVisitor;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.hazelcast.sql.impl.plan.node.PlanNodeFieldTypeProvider.FAILING_FIELD_TYPE_PROVIDER;
import static java.util.stream.Collectors.toMap;

public class UpdateByKeyMapPhysicalRel extends AbstractRelNode implements PhysicalRel {

    private final RelOptTable table;
    private final RexNode keyCondition;
    private final List<String> updatedColumns;
    private final List<RexNode> sourceExpressions;

    UpdateByKeyMapPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            RexNode keyCondition,
            List<String> updatedColumns,
            List<RexNode> sourceExpressions
    ) {
        super(cluster, traitSet);

        assert table.unwrap(HazelcastTable.class).getTarget() instanceof PartitionedMapTable;

        this.table = table;
        this.keyCondition = keyCondition;
        this.updatedColumns = updatedColumns;
        this.sourceExpressions = sourceExpressions;
    }

    public String mapName() {
        return table().getMapName();
    }

    public PlanObjectKey objectKey() {
        return table().getObjectKey();
    }

    public Expression<?> keyCondition(QueryParameterMetadata parameterMetadata) {
        RexToExpressionVisitor visitor = new RexToExpressionVisitor(FAILING_FIELD_TYPE_PROVIDER, parameterMetadata);
        return keyCondition.accept(visitor);
    }

    public UpdatingEntryProcessor.Supplier updaterSupplier(QueryParameterMetadata parameterMetadata) {
        List<Expression<?>> projects = project(OptUtils.schema(table), sourceExpressions, parameterMetadata);
        Map<String, Expression<?>> updates = IntStream.range(0, projects.size())
                .boxed()
                .collect(toMap(updatedColumns::get, projects::get));
        return UpdatingEntryProcessor.supplier(table(), updates);
    }

    private PartitionedMapTable table() {
        return table.unwrap(HazelcastTable.class).getTarget();
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
        return RelOptUtil.createDmlRowType(SqlKind.UPDATE, getCluster().getTypeFactory());
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return pw
                .item("table", table.getQualifiedName())
                .item("keyCondition", keyCondition)
                .item("updatedColumns", updatedColumns)
                .item("sourceExpressions", sourceExpressions);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new UpdateByKeyMapPhysicalRel(getCluster(), traitSet, table, keyCondition, updatedColumns, sourceExpressions);
    }
}
