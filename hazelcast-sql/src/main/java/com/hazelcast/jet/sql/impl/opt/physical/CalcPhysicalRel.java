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

import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;

import java.util.List;

import static com.hazelcast.jet.impl.util.Util.toList;

public class CalcPhysicalRel extends Calc implements PhysicalRel {

    CalcPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            RexProgram program
    ) {
        super(cluster, traits, input, program);
    }

    public RexNode filter() {
        return program.expandLocalRef(program.getCondition());
    }

    public List<RexNode> projection() {
        return program.expandList(program.getProjectList());
    }

    @Override
    public PlanNodeSchema schema(QueryParameterMetadata parameterMetadata) {
        List<QueryDataType> fieldTypes = toList(projection(), n -> HazelcastTypeUtils.toHazelcastType(n.getType()));
        return new PlanNodeSchema(fieldTypes);
    }

    @Override
    public <V> V accept(CreateDagVisitor<V> visitor) {
        return visitor.onCalc(this);
    }

    @Override
    public final RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw);
    }

    @Override
    public final Calc copy(RelTraitSet traitSet, RelNode input, RexProgram program) {
        return new CalcPhysicalRel(getCluster(), traitSet, input, program);
    }
}
