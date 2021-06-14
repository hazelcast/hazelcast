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
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.schema.TableField;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;

public class UpdatePhysicalRel extends TableModify implements PhysicalRel {

    private final List<RexNode> projects;

    UpdatePhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            Prepare.CatalogReader catalogReader,
            RelNode input,
            Operation operation,
            List<String> updateColumnList,
            List<RexNode> sourceExpressionList,
            boolean flattened,
            List<RexNode> projects
    ) {
        super(cluster, traitSet, table, catalogReader, input, operation, updateColumnList, sourceExpressionList, flattened);

        this.projects = projects;
    }

    public List<String> updatedFields() {
        return getUpdateColumnList();
    }

    // input = all table fields + joined fields (if any)
    // inputProjections = all table fields + joined fields (if any) + updated fields
    public List<Expression<?>> updates(QueryParameterMetadata parameterMetadata) {
        int inputFieldCount = getInput().getRowType().getFieldCount();
        PlanNodeSchema inputSchema = ((PhysicalRel) getInput()).schema(parameterMetadata);
        List<Expression<?>> inputProjections = project(inputSchema, this.projects, parameterMetadata);

        List<TableField> fields = getTable().unwrap(HazelcastTable.class).getTarget().getFields();

        List<Expression<?>> updates = new ArrayList<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            TableField field = fields.get(i);

            int updatedColumnIndex = getUpdateColumnList().indexOf(field.getName());
            if (updatedColumnIndex > -1) {
                updates.add(inputProjections.get(inputFieldCount + updatedColumnIndex));
            } else if (field.isHidden()) {
                updates.add(ConstantExpression.create(null, field.getType()));
            } else {
                updates.add(inputProjections.get(i));
            }
        }
        return updates;
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
                isFlattened(),
                projects
        );
    }
}
