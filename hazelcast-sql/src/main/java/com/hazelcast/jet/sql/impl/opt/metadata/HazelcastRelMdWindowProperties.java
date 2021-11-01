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

package com.hazelcast.jet.sql.impl.opt.metadata;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.SlidingWindowPolicy;
import com.hazelcast.jet.sql.impl.opt.metadata.WindowProperties.WindowEndProperty;
import com.hazelcast.jet.sql.impl.opt.metadata.WindowProperties.WindowProperty;
import com.hazelcast.jet.sql.impl.opt.metadata.WindowProperties.WindowStartProperty;
import com.hazelcast.jet.sql.impl.opt.physical.SlidingWindowPhysicalRel;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.HazelcastRelOptCluster;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Util;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;

public final class HazelcastRelMdWindowProperties
        implements MetadataHandler<HazelcastRelMdWindowProperties.WindowPropertiesMetadata> {

    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(
            WindowPropertiesMetadata.METHOD,
            new HazelcastRelMdWindowProperties()
    );

    private HazelcastRelMdWindowProperties() {
    }

    @Override
    public MetadataDef<WindowPropertiesMetadata> getDef() {
        return WindowPropertiesMetadata.DEF;
    }

    @SuppressWarnings("unused")
    public WindowProperties extractWindowProperties(SlidingWindowPhysicalRel rel, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        WindowProperties inputWindowProperties = query.extractWindowProperties(rel.getInput());

        int fieldCount = rel.getRowType().getFieldCount();
        FunctionEx<ExpressionEvalContext, SlidingWindowPolicy> windowPolicyProvider = rel.windowPolicyProvider();
        WindowProperties windowProperties = new WindowProperties(
                // window_start and window_end are the last two fields in SlidingWindowPhysicalRel
                new WindowStartProperty(fieldCount - 2, windowPolicyProvider),
                new WindowEndProperty(fieldCount - 1, windowPolicyProvider)
        );

        return inputWindowProperties == null ? windowProperties : inputWindowProperties.merge(windowProperties);
    }

    @SuppressWarnings("unused")
    public WindowProperties extractWindowProperties(Project rel, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        WindowProperties inputWindowProperties = query.extractWindowProperties(rel.getInput());

        if (inputWindowProperties == null) {
            return null;
        } else {
            Map<Integer, List<Integer>> projections = toProjections(rel.getProjects());
            WindowProperty[] windowProperties = inputWindowProperties.getProperties()
                    .flatMap(property ->
                            projections.getOrDefault(property.index(), emptyList()).stream().map(property::withIndex)
                    ).toArray(WindowProperty[]::new);
            return new WindowProperties(windowProperties);
        }
    }

    private Map<Integer, List<Integer>> toProjections(List<RexNode> projects) {
        final class ProjectFieldVisitor extends RexVisitorImpl<Integer> {

            private ProjectFieldVisitor() {
                super(false);
            }

            @Override
            public Integer visitInputRef(RexInputRef input) {
                return input.getIndex();
            }

            @Override
            public Integer visitCall(RexCall call) {
                if (call.getKind() == SqlKind.AS) {
                    return call.getOperands().get(0).accept(this);
                }

                // any operation on the window field loses the window attribute, even if it's a simple cast

                return null;
            }
        }

        RexVisitor<Integer> visitor = new ProjectFieldVisitor();
        Map<Integer, List<Integer>> projections = new HashMap<>();
        for (int i = 0; i < projects.size(); i++) {
            Integer index = projects.get(i).accept(visitor);
            if (index != null) {
                projections.computeIfAbsent(index, key -> new ArrayList<>()).add(i);
            }
        }
        return projections;
    }

    @SuppressWarnings("unused")
    public WindowProperties extractWindowProperties(Aggregate rel, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        WindowProperties inputWindowProperties = query.extractWindowProperties(rel.getInput());

        return inputWindowProperties == null ? null : inputWindowProperties.retain(rel.getGroupSet().asSet());
    }

    @SuppressWarnings("unused")
    public WindowProperties extractWindowProperties(Join rel, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        WindowProperties leftInputWindowProperties = query.extractWindowProperties(rel.getLeft());
        WindowProperties rightInputWindowProperties = query.extractWindowProperties(rel.getRight());

        if (rightInputWindowProperties == null) {
            return leftInputWindowProperties;
        }

        int leftCount = rel.getLeft().getRowType().getFieldCount();
        WindowProperty[] windowProperties = rightInputWindowProperties.getProperties()
                .map(property -> property.withIndex(leftCount + property.index()))
                .toArray(WindowProperty[]::new);
        WindowProperties rightWindowProperties = new WindowProperties(windowProperties);

        return leftInputWindowProperties == null
                ? rightWindowProperties
                : leftInputWindowProperties.merge(rightWindowProperties);
    }

    // i.e. Filter, AggregateAccumulateByKeyPhysicalRel, AggregateAccumulatePhysicalRel
    @SuppressWarnings("unused")
    public WindowProperties extractWindowProperties(SingleRel rel, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        return query.extractWindowProperties(rel.getInput());
    }

    @SuppressWarnings("unused")
    public WindowProperties extractWindowProperties(RelSubset subset, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        RelNode rel = Util.first(subset.getBest(), subset.getOriginal());
        return query.extractWindowProperties(rel);
    }

    @SuppressWarnings("unused")
    public WindowProperties extractWindowProperties(RelNode rel, RelMetadataQuery mq) {
        return null;
    }

    public interface WindowPropertiesMetadata extends Metadata {

        Method METHOD = Types.lookupMethod(WindowPropertiesMetadata.class, "extractWindowProperties");

        MetadataDef<WindowPropertiesMetadata> DEF = MetadataDef.of(
                WindowPropertiesMetadata.class,
                WindowPropertiesMetadata.Handler.class,
                METHOD
        );

        @SuppressWarnings("unused")
        WindowProperties extractWindowProperties();

        interface Handler extends MetadataHandler<WindowPropertiesMetadata> {

            WindowProperties extractWindowProperties(RelNode rel, RelMetadataQuery mq);
        }
    }
}
