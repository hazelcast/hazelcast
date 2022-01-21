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
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvProjector;
import com.hazelcast.jet.sql.impl.inject.UpsertTargetDescriptor;
import com.hazelcast.jet.sql.impl.opt.ExpressionValues;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
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
import org.apache.calcite.sql.SqlKind;

import java.util.List;
import java.util.Map.Entry;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;

public class InsertMapPhysicalRel extends AbstractRelNode implements PhysicalRel {

    private final RelOptTable table;
    private final ExpressionValues values;

    InsertMapPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            ExpressionValues values
    ) {
        super(cluster, traitSet);

        assert table.unwrap(HazelcastTable.class).getTarget() instanceof PartitionedMapTable;

        this.table = table;
        this.values = values;
    }

    public String mapName() {
        return table().getMapName();
    }

    public PlanObjectKey objectKey() {
        return table().getObjectKey();
    }

    public Function<ExpressionEvalContext, List<Entry<Object, Object>>> entriesFn() {
        PartitionedMapTable table = table();
        ExpressionValues values = this.values;
        return evalContext -> {
            KvProjector projector = KvProjector.supplier(
                    table.paths(),
                    table.types(),
                    (UpsertTargetDescriptor) table.getKeyJetMetadata(),
                    (UpsertTargetDescriptor) table.getValueJetMetadata(),
                    true
            ).get(evalContext.getSerializationService());

            return values.toValues(evalContext)
                    .map(projector::project)
                    .collect(toList());
        };
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
        return RelOptUtil.createDmlRowType(SqlKind.INSERT, getCluster().getTypeFactory());
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return pw
                .item("table", table.getQualifiedName())
                .item("values", values);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new InsertMapPhysicalRel(getCluster(), traitSet, table, values);
    }
}
