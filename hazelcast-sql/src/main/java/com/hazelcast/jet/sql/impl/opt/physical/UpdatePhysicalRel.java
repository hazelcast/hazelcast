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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.hazelcast.jet.impl.util.Util.toList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

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

    public Map<String, Expression<?>> updates(QueryParameterMetadata parameterMetadata) {
        RexBuilder rexBuilder = getCluster().getRexBuilder();
        RelDataTypeFactory typeFactory = getCluster().getTypeFactory();

        List<RelDataTypeField> fields = getTable().unwrap(HazelcastTable.class).getRowType(typeFactory).getFieldList();
        List<RelDataType> fieldTypes = toList(fields, RelDataTypeField::getType);
        List<String> fieldNames = toList(fields, RelDataTypeField::getName);

        List<RexNode> nodes = IntStream.range(0, fields.size())
                .mapToObj(i -> {
                    int updatedColumnIndex = getUpdateColumnList().indexOf(fieldNames.get(i));
                    return updatedColumnIndex > -1
                            ? getSourceExpressionList().get(updatedColumnIndex)
                            : rexBuilder.makeInputRef(fieldTypes.get(i), i);
                }).collect(toList());
        List<Expression<?>> projections = project(OptUtils.schema(getTable()), nodes, parameterMetadata);

        return getUpdateColumnList().stream()
                .collect(toMap(identity(), fieldName -> projections.get(fieldNames.indexOf(fieldName))));
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
