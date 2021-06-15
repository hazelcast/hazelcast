/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;

public class UpdatePhysicalRel extends TableModify implements PhysicalRel {

    UpdatePhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            Prepare.CatalogReader catalogReader,
            RelNode input,
            Operation operation,
            List<String> updateColumnList,
            List<RexNode> sourceExpressionList,
            boolean flattened
    ) {
        super(cluster, traitSet, table, catalogReader, input, operation, updateColumnList, sourceExpressionList, flattened);
    }

    public List<String> updatedFields() {
        return getUpdateColumnList();
    }

    public List<Expression<?>> updates(QueryParameterMetadata parameterMetadata) {
        RexBuilder rexBuilder = getCluster().getRexBuilder();

        HazelcastTable hazelcastTable = getTable().unwrap(HazelcastTable.class);
        List<RelDataTypeField> hazelcastTableFields = hazelcastTable.getRowType(getCluster().getTypeFactory()).getFieldList();
        Table table = hazelcastTable.getTarget();

        List<RexNode> updates = new ArrayList<>(table.getFieldCount());
        for (int i = 0; i < table.getFieldCount(); i++) {
            TableField field = table.getField(i);

            int updatedColumnIndex = getUpdateColumnList().indexOf(field.getName());
            if (updatedColumnIndex > -1) {
                updates.add(getSourceExpressionList().get(updatedColumnIndex));
            } else if (field.isHidden()) {
                updates.add(rexBuilder.makeNullLiteral(hazelcastTableFields.get(i).getType()));
            } else {
                updates.add(rexBuilder.makeInputRef(hazelcastTableFields.get(i).getType(), i));
            }
        }
        PlanNodeSchema schema = OptUtils.schema(getTable());
        return project(schema, updates, parameterMetadata);
    }

    @Override
    public PlanNodeSchema schema(QueryParameterMetadata parameterMetadata) {
        return OptUtils.schema(getTable());
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
                getOperation(),
                getUpdateColumnList(),
                getSourceExpressionList(),
                isFlattened()
        );
    }
}
