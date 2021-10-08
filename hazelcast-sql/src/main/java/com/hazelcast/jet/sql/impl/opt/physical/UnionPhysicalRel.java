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
import com.hazelcast.jet.sql.impl.opt.logical.FullScanLogicalRel;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeFieldTypeProvider;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.hazelcast.jet.impl.util.Util.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.calcite.rel.core.TableModify.Operation.UPDATE;

public class UnionPhysicalRel extends Union implements PhysicalRel {

    UnionPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelNode> inputs,
            boolean all
    ) {
        super(cluster, traitSet, inputs, all);
    }

    @Override
    public PlanNodeSchema schema(QueryParameterMetadata parameterMetadata) {
        List<QueryDataType> fieldTypes = toList(projection(parameterMetadata), Expression::getType);
        return new PlanNodeSchema(fieldTypes);
    }

    public List<Expression<?>> projection(QueryParameterMetadata parameterMetadata) {
        assert inputs.size() > 0;
        // To combine the result sets of two queries using the UNION operator,
        // the queries must conform to the following rules:
        // The number and the order of the columns in the select list of both queries must be the same.
        // The data types must be compatible.

        // Find the only FullScan/IndexScan relation
        // For multiple UNION statements one of two possible inputs would be ScanPhysicalRel
        RelOptTable relTable = null;
        for (int i = 0; i < inputs.size(); ++i) {
            if (inputs.get(i) instanceof FullScanPhysicalRel || inputs.get(i) instanceof IndexScanMapPhysicalRel) {
                relTable = inputs.get(i).getTable();
            }
        }
        assert relTable != null;

        PlanNodeSchema schema = OptUtils.schema(relTable);

        HazelcastTable table = relTable.unwrap(HazelcastTable.class);

        List<Integer> projects = table.getProjects();
        List<RexNode> projection = new ArrayList<>(projects.size());
        for (Integer index : projects) {
            TableField field = table.getTarget().getField(index);
            RelDataType relDataType = OptUtils.convert(field, getCluster().getTypeFactory());
            projection.add(new RexInputRef(index, relDataType));
        }

        return project(schema, projection, parameterMetadata);
    }

    @Override
    public Vertex accept(CreateDagVisitor visitor) {
        return visitor.onUnion(this);
    }

    @Override
    public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new UnionPhysicalRel(
                getCluster(),
                traitSet,
                inputs,
                all
        );
    }
}
