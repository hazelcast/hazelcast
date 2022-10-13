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

import com.google.common.collect.ImmutableList;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mappings;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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

    public Expression<Boolean> filter(QueryParameterMetadata parameterMetadata) {
        PlanNodeSchema schema = ((PhysicalRel) getInput()).schema(parameterMetadata);
        return filter(schema, program.expandLocalRef(program.getCondition()), parameterMetadata);
    }

    public List<Expression<?>> projection(QueryParameterMetadata parameterMetadata) {
        PlanNodeSchema inputSchema = ((PhysicalRel) getInput()).schema(parameterMetadata);
        List<RexNode> projectList = program.expandList(program.getProjectList());
        return project(inputSchema, projectList, parameterMetadata);
    }

    @Override
    public PlanNodeSchema schema(QueryParameterMetadata parameterMetadata) {
        List<QueryDataType> fieldTypes = toList(projection(parameterMetadata), Expression::getType);
        return new PlanNodeSchema(fieldTypes);
    }

    @Override
    public Vertex accept(CreateDagVisitor visitor) {
        return visitor.onCalc(this);
    }

    @Override
    public RelTraitSet passThroughCollationTraits(RelNode rel, RelTraitSet required) {
        RelCollation collation = required.getCollation();
        if (collation == null) {
            return required;
        }

        assert rel instanceof CalcPhysicalRel;
        CalcPhysicalRel calc = (CalcPhysicalRel) rel;

        List<Integer> fieldProjects = calc.getProgram().expandList(calc.getProgram().getProjectList())
                .stream().filter(expr -> expr instanceof RexInputRef)
                .map(inputRef -> ((RexInputRef) inputRef).getIndex())
                .collect(Collectors.toList());


        List<Integer> fieldOrdinals = collation.getFieldCollations()
                .stream().map(RelFieldCollation::getFieldIndex)
                .collect(Collectors.toList());

        List<RelFieldCollation> fields = new ArrayList<>();

        for (int i = 0; i < fieldOrdinals.size(); ++i) {
            Integer indexFieldOrdinal = fieldOrdinals.get(i);

            int remappedIndexFieldOrdinal = fieldProjects.indexOf(indexFieldOrdinal);
            if (remappedIndexFieldOrdinal == -1) {
                // The field is not used in the query
                break;
            }
            RelFieldCollation.Direction direction = collation.getFieldCollations().get(i).getDirection();
            RelFieldCollation fieldCollation = new RelFieldCollation(remappedIndexFieldOrdinal, direction);
            fields.add(fieldCollation);
        }

        return OptUtils.traitPlus(required, RelCollations.of(fields));
    }

    @Override
    public @Nullable Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        RelCollation collation = OptUtils.collation(childTraits);
        if (collation.getFieldCollations().isEmpty()) {
            return Pair.of(childTraits.replace(RelCollations.EMPTY), ImmutableList.of(childTraits));
        }

        List<RexNode> projects = Util.transform(program.getProjectList(), program::expandLocalRef);
        RelDataType inputRowType = program.getInputRowType();
        Mappings.TargetMapping mapping = RelOptUtil.permutationPushDownProject(projects, inputRowType, 0, 0);

        RelTrait transformedCollation = collation.apply(mapping);
        return Pair.of(childTraits.replace(transformedCollation), ImmutableList.of(childTraits));
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
