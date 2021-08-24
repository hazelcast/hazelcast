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
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.jet.sql.impl.opt.physical.visitor.RexToExpressionVisitor;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.util.Util.toList;
import static com.hazelcast.sql.impl.plan.node.PlanNodeFieldTypeProvider.FAILING_FIELD_TYPE_PROVIDER;

public class SelectByKeyMapPhysicalRel extends AbstractRelNode implements PhysicalRel {

    private final RelOptTable table;
    private final RexNode keyCondition;
    private final List<? extends RexNode> projections;

    SelectByKeyMapPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelDataType rowType,
            RelOptTable table,
            RexNode keyCondition,
            List<? extends RexNode> projections
    ) {
        super(cluster, traitSet);
        this.rowType = rowType;

        assert table.unwrap(HazelcastTable.class).getTarget() instanceof PartitionedMapTable;

        this.table = table;
        this.keyCondition = keyCondition;
        this.projections = projections;
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

    public KvRowProjector.Supplier rowProjectorSupplier(QueryParameterMetadata parameterMetadata) {
        PartitionedMapTable table = table();
        return KvRowProjector.supplier(
                table.paths(),
                table.types(),
                table.getKeyDescriptor(),
                table.getValueDescriptor(),
                null,
                projection(parameterMetadata)
        );
    }

    private PartitionedMapTable table() {
        return table.unwrap(HazelcastTable.class).getTarget();
    }

    private List<Expression<?>> projection(QueryParameterMetadata parameterMetadata) {
        PlanNodeSchema inputSchema = OptUtils.schema(table);
        return project(inputSchema, projections, parameterMetadata);
    }

    @Override
    public PlanNodeSchema schema(QueryParameterMetadata parameterMetadata) {
        List<QueryDataType> fieldTypes = toList(projection(parameterMetadata), Expression::getType);
        return new PlanNodeSchema(fieldTypes);
    }

    @Override
    public Vertex accept(CreateDagVisitor visitor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return pw
                .item("table", table.getQualifiedName())
                .item("keyCondition", keyCondition)
                .item("projections", Ord.zip(rowType.getFieldList()).stream()
                        .map(field -> {
                            String fieldName = field.e.getName() == null ? "field#" + field.i : field.e.getName();
                            return fieldName + "=[" + projections.get(field.i) + "]";
                        }).collect(Collectors.joining(", "))
                );
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new SelectByKeyMapPhysicalRel(getCluster(), traitSet, rowType, table, keyCondition, projections);
    }
}
