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
import com.hazelcast.jet.cascading.planner.rule.tez.annotator.AccumulatedPostNodeAnnotator;
import com.hazelcast.jet.cascading.planner.rule.tez.assertion.DualStreamedAccumulatedMergeNodeAssert;
import com.hazelcast.jet.cascading.planner.rule.tez.partitioner.BottomUpBoundariesNodePartitioner;
import com.hazelcast.jet.cascading.planner.rule.tez.partitioner.BottomUpJoinedBoundariesNodePartitioner;
import com.hazelcast.jet.cascading.planner.rule.tez.partitioner.ConsecutiveGroupOrMergesNodePartitioner;
import com.hazelcast.jet.cascading.planner.rule.tez.partitioner.SplitJoinBoundariesNodeRePartitioner;
import com.hazelcast.jet.cascading.planner.rule.tez.partitioner.StreamedAccumulatedBoundariesNodeRePartitioner;
import com.hazelcast.jet.cascading.planner.rule.tez.partitioner.StreamedOnlySourcesNodeRePartitioner;
import com.hazelcast.jet.cascading.planner.rule.tez.partitioner.TopDownSplitBoundariesNodePartitioner;
import com.hazelcast.jet.cascading.planner.rule.tez.transformer.BoundaryBalanceCheckpointTransformer;
import com.hazelcast.jet.cascading.planner.rule.tez.transformer.BoundaryBalanceGroupBlockingHashJoinTransformer;
import com.hazelcast.jet.cascading.planner.rule.tez.transformer.BoundaryBalanceGroupSplitHashJoinTransformer;
import com.hazelcast.jet.cascading.planner.rule.tez.transformer.BoundaryBalanceGroupSplitSpliceTransformer;
import com.hazelcast.jet.cascading.planner.rule.tez.transformer.BoundaryBalanceGroupSplitTransformer;
import com.hazelcast.jet.cascading.planner.rule.tez.transformer.BoundaryBalanceHashJoinSameSourceTransformer;
import com.hazelcast.jet.cascading.planner.rule.tez.transformer.BoundaryBalanceHashJoinToHashJoinTransformer;
import com.hazelcast.jet.cascading.planner.rule.tez.transformer.BoundaryBalanceJoinSplitTransformer;
import com.hazelcast.jet.cascading.planner.rule.tez.transformer.BoundaryBalanceSplitSplitToStreamedHashJoinTransformer;
import com.hazelcast.jet.cascading.planner.rule.tez.transformer.BoundaryBalanceSplitToStreamedHashJoinTransformer;
import com.hazelcast.jet.cascading.planner.rule.tez.transformer.RemoveMalformedHashJoinNodeTransformer;

/**
 * The HashJoinHadoop2TezRuleRegistry provides support for assemblies using {@link cascading.pipe.HashJoin} pipes.
 * <p/>
 * Detecting and optimizing for HashJoin pipes adds further complexity and time to converge on a valid physical plan.
 * <p/>
 * If facing slowdowns, and no HashJoins are used, switch to the
 * {@link cascading.flow.tez.planner.NoHashJoinHadoop2TezRuleRegistry} via the appropriate
 * {@link cascading.flow.FlowConnector} constructor.
 */
@SuppressWarnings(value = {"checkstyle:classdataabstractioncoupling",
        "checkstyle:executablestatementcount", "checkstyle:trailingcomment"})
public class HashJoinHadoop2TezRuleRegistry extends RuleRegistry {
    public HashJoinHadoop2TezRuleRegistry() {
//    enableDebugLogging();

        // PreBalance
        addRule(new LoneGroupAssert());
        addRule(new MissingGroupAssert());
        addRule(new BufferAfterEveryAssert());
        addRule(new EveryAfterBufferAssert());
        addRule(new SplitBeforeEveryAssert());

        addRule(new BoundaryBalanceGroupSplitTransformer());
        // prevents AssemblyHelpersPlatformTest#testSameSourceMerge deadlock
        addRule(new BoundaryBalanceGroupSplitSpliceTransformer());
        addRule(new BoundaryBalanceCheckpointTransformer());

        // hash join
        addRule(new BoundaryBalanceHashJoinSameSourceTransformer());
        addRule(new BoundaryBalanceSplitToStreamedHashJoinTransformer()); // testGroupBySplitGroupByJoin
        addRule(new BoundaryBalanceSplitSplitToStreamedHashJoinTransformer()); // testGroupBySplitSplitGroupByJoin
        addRule(new BoundaryBalanceHashJoinToHashJoinTransformer()); // force HJ into unique nodes
        addRule(new BoundaryBalanceGroupBlockingHashJoinTransformer()); // joinAfterEvery
        addRule(new BoundaryBalanceGroupSplitHashJoinTransformer()); // groupBySplitJoins
        addRule(new BoundaryBalanceJoinSplitTransformer()); // prevents duplication of HashJoin, testJoinSplit

        // PreResolve
        addRule(new RemoveNoOpPipeTransformer());
        addRule(new ApplyAssertionLevelTransformer());
        addRule(new ApplyDebugLevelTransformer());

        // PostResolve

        // PartitionSteps
        addRule(new WholeGraphStepPartitioner());

        // PostSteps

        // PartitionNodes

        // no match with HashJoin inclusion
        addRule(new TopDownSplitBoundariesNodePartitioner()); // split from source to multiple sinks
        addRule(new ConsecutiveGroupOrMergesNodePartitioner());
        addRule(new BottomUpBoundariesNodePartitioner()); // streamed paths re-partitioned w/ StreamedOnly
        addRule(new SplitJoinBoundariesNodeRePartitioner()); // testCoGroupSelf - compensates for tez-1190

        // hash join inclusion
        // will capture multiple inputs into sink for use with HashJoins
        addRule(new BottomUpJoinedBoundariesNodePartitioner());
        addRule(new StreamedAccumulatedBoundariesNodeRePartitioner()); // joinsIntoCoGroupLhs & groupBySplitJoins
        addRule(new StreamedOnlySourcesNodeRePartitioner());

        // PostNodes
        addRule(new RemoveMalformedHashJoinNodeTransformer()); // joinsIntoCoGroupLhs
        addRule(new AccumulatedPostNodeAnnotator()); // allows accumulated boundaries to be identified

        addRule(new DualStreamedAccumulatedMergeNodeAssert());
    }
}
