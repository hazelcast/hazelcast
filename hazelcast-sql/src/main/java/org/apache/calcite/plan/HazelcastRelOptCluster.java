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

package org.apache.calcite.plan;

import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.jet.sql.impl.opt.distribution.DistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Extended {@link RelOptCluster} with Hazelcast-specific data.
 * <p>
 * Located in the Calcite package because the required super constructor is package-private.
 */
public final class HazelcastRelOptCluster extends RelOptCluster {
    /** Distribution trait definition that is used during query execution. */
    private final DistributionTraitDef distributionTraitDef;

    /** Metadata about parameters. */
    private QueryParameterMetadata parameterMetadata;

    /** Whether 'CREATE JOB' is used */
    private boolean requiresJob;

    private HazelcastRelOptCluster(
            RelOptPlanner planner,
            RelDataTypeFactory typeFactory,
            RexBuilder rexBuilder,
            AtomicInteger nextCorrel,
            Map<String, RelNode> mapCorrelToRel,
            DistributionTraitDef distributionTraitDef
    ) {
        super(planner, typeFactory, rexBuilder, nextCorrel, mapCorrelToRel);

        this.distributionTraitDef = distributionTraitDef;
    }

    public static HazelcastRelOptCluster create(
            RelOptPlanner planner,
            RexBuilder rexBuilder,
            DistributionTraitDef distributionTraitDef
    ) {
        return new HazelcastRelOptCluster(
                planner,
                rexBuilder.getTypeFactory(),
                rexBuilder,
                new AtomicInteger(0),
                new HashMap<>(),
                distributionTraitDef
        );
    }

    public DistributionTraitDef getDistributionTraitDef() {
        return distributionTraitDef;
    }

    public QueryParameterMetadata getParameterMetadata() {
        return parameterMetadata;
    }

    public void setParameterMetadata(QueryParameterMetadata parameterMetadata) {
        this.parameterMetadata = parameterMetadata;
    }

    public boolean requiresJob() {
        return requiresJob;
    }

    public void setRequiresJob(boolean requiresJob) {
        this.requiresJob = requiresJob;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{distributionTraitDef=" + distributionTraitDef + '}';
    }
}
