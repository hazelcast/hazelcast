/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.cascading.planner.rule.tez;

import cascading.flow.planner.rule.RuleRegistry;
import cascading.flow.planner.rule.assertion.BufferAfterEveryAssert;
import cascading.flow.planner.rule.assertion.EveryAfterBufferAssert;
import cascading.flow.planner.rule.assertion.LoneGroupAssert;
import cascading.flow.planner.rule.assertion.MissingGroupAssert;
import cascading.flow.planner.rule.assertion.SplitBeforeEveryAssert;
import cascading.flow.planner.rule.partitioner.WholeGraphStepPartitioner;
import cascading.flow.planner.rule.transformer.ApplyAssertionLevelTransformer;
import cascading.flow.planner.rule.transformer.ApplyDebugLevelTransformer;
import cascading.flow.planner.rule.transformer.RemoveNoOpPipeTransformer;
import com.hazelcast.jet.cascading.planner.rule.tez.assertion.NoHashJoinAssert;
import com.hazelcast.jet.cascading.planner.rule.tez.partitioner.ConsecutiveGroupOrMergesNodePartitioner;
import com.hazelcast.jet.cascading.planner.rule.tez.partitioner.SplitJoinBoundariesNodeRePartitioner;
import com.hazelcast.jet.cascading.planner.rule.tez.partitioner.TopDownBoundariesNodePartitioner;
import com.hazelcast.jet.cascading.planner.rule.tez.transformer.BoundaryBalanceCheckpointTransformer;
import com.hazelcast.jet.cascading.planner.rule.tez.transformer.BoundaryBalanceGroupSplitSpliceTransformer;

/**
 * The NoHashJoinHadoop2TezRuleRegistry assumes the plan has no {@link cascading.pipe.HashJoin} Pipes in the
 * assembly, otherwise an planner failure will be thrown.
 * <p/>
 * This rule registry can be used if the default registry is failing or producing less than optimal plans.
 *
 * @see cascading.flow.tez.planner.HashJoinHadoop2TezRuleRegistry
 */
public class NoHashJoinHadoop2TezRuleRegistry extends RuleRegistry {
    public NoHashJoinHadoop2TezRuleRegistry() {
//    enableDebugLogging();

        // PreBalance
        // fail if we encounter a HashJoin
        addRule(new NoHashJoinAssert());

        addRule(new LoneGroupAssert());
        addRule(new MissingGroupAssert());
        addRule(new BufferAfterEveryAssert());
        addRule(new EveryAfterBufferAssert());
        addRule(new SplitBeforeEveryAssert());

        // prevents AssemblyHelpersPlatformTest#testSameSourceMerge deadlock
        addRule(new BoundaryBalanceGroupSplitSpliceTransformer());
        addRule(new BoundaryBalanceCheckpointTransformer());

        // PreResolve
        addRule(new RemoveNoOpPipeTransformer());
        addRule(new ApplyAssertionLevelTransformer());
        addRule(new ApplyDebugLevelTransformer());

        // PostResolve

        // PartitionSteps
        addRule(new WholeGraphStepPartitioner());

        // PostSteps

        // PartitionNodes
        addRule(new TopDownBoundariesNodePartitioner());
        addRule(new ConsecutiveGroupOrMergesNodePartitioner());

        // testCoGroupSelf - compensates for tez-1190
        addRule(new SplitJoinBoundariesNodeRePartitioner());

        // PostNodes
    }
}
