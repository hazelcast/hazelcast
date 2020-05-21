/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.opt.physical.join;

import com.hazelcast.sql.impl.calcite.opt.HazelcastConventions;
import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.opt.distribution.DistributionTrait;
import com.hazelcast.sql.impl.calcite.opt.distribution.DistributionTraitDef;
import com.hazelcast.sql.impl.calcite.opt.distribution.DistributionType;
import com.hazelcast.sql.impl.calcite.opt.logical.JoinLogicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.MaterializedInputPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.ReplicatedToDistributedPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.BroadcastExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.UnicastExchangePhysicalRel;
import org.apache.calcite.plan.HazelcastRelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * General rule for join processing.
 */
public final class JoinPhysicalRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new JoinPhysicalRule();

    private JoinPhysicalRule() {
        super(
            OptUtils.parentChildChild(JoinLogicalRel.class, RelNode.class, RelNode.class, HazelcastConventions.LOGICAL),
            JoinPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        JoinLogicalRel logicalJoin = call.rel(0);

        JoinType type = getJoinType(logicalJoin.getJoinType());

        if (type == null) {
            return;
        }

        JoinConditionType conditionType = logicalJoin.hasEquiJoinKeys() ? JoinConditionType.EQUI : JoinConditionType.OTHER;

        RelNode convertedLeft = OptUtils.toPhysicalInput(logicalJoin.getLeft());
        RelNode convertedRight = OptUtils.toPhysicalInput(logicalJoin.getRight());

        Collection<RelNode> leftCandidates = OptUtils.getPhysicalRelsFromSubset(convertedLeft);
        Collection<RelNode> rightCandidates = OptUtils.getPhysicalRelsFromSubset(convertedRight);

        List<RelNode> transforms = new ArrayList<>(1);

        if (!leftCandidates.isEmpty() && !rightCandidates.isEmpty()) {
            for (RelNode leftCandidate : leftCandidates) {
                for (RelNode rightCandidate : rightCandidates) {
                    List<RelNode> candidateTransforms = createTransforms(
                        type,
                        conditionType,
                        logicalJoin,
                        leftCandidate,
                        rightCandidate
                    );

                    transforms.addAll(candidateTransforms);
                }
            }
        }

        for (RelNode transform : transforms) {
            call.transformTo(transform);
        }
    }

    /**
     * Create transformation for the given pair of inputs.
     *
     * @param type Join type.
     * @param logicalJoin Logical join.
     * @param left Left physical input.
     * @param right Right physical input.
     * @return Physical join.
     */
    private List<RelNode> createTransforms(
        JoinType type,
        JoinConditionType conditionType,
        JoinLogicalRel logicalJoin,
        RelNode left,
        RelNode right
    ) {
        if (conditionType == JoinConditionType.OTHER) {
            return createTransformsNonEquiJoin(type, logicalJoin, left, right);
        } else {
            assert conditionType == JoinConditionType.EQUI;

            return createTransformsEquiJoin(type, logicalJoin, left, right);
        }
    }

    /**
     * Create transformations for non-equi join.
     *
     * @param type Join type.
     * @param logicalJoin Logical join.
     * @param left Left input.
     * @param right Right input.
     * @return Transformations.
     */
    private List<RelNode> createTransformsNonEquiJoin(
        JoinType type,
        JoinLogicalRel logicalJoin,
        RelNode left,
        RelNode right
    ) {
        DistributionTrait leftDist = OptUtils.getDistribution(left);
        DistributionTrait rightDist = OptUtils.getDistribution(right);

        JoinDistribution leftJoinDist = joinDistributionForNonEquiJoin(leftDist);
        JoinDistribution rightJoinDist = joinDistributionForNonEquiJoin(rightDist);

        List<JoinCollocationStrategy> strategies = JoinCollocationStrategy.resolve(
            OptUtils.getDistributionDef(left).getMemberCount(),
            type,
            JoinConditionType.OTHER,
            leftJoinDist,
            rightJoinDist
        );

        List<RelNode> res = new ArrayList<>(strategies.size());

        for (JoinCollocationStrategy strategy : strategies) {
            RelNode node = createTransformNonEquiJoin(type, logicalJoin, left, right, strategy);

            res.add(node);
        }

        return res;
    }

    /**
     * Create transform for non-equi join. Non-equi joins may only use broadcast and resulting distribution is always
     * either REPLICATED or RANDOM.
     *
     * @param type Join type.
     * @param logicalJoin Logical join.
     * @param left Left input.
     * @param right Right input.
     * @param strategy Strategy.
     * @return Result.
     */
    private RelNode createTransformNonEquiJoin(
        JoinType type,
        JoinLogicalRel logicalJoin,
        RelNode left,
        RelNode right,
        JoinCollocationStrategy strategy
    ) {
        // Step 1: Create inputs for join.
        RelNode leftCollocated = createCollocatedInputNonEquiJoin(left, strategy.getLeftAction(), true);
        RelNode rightCollocated = createCollocatedInputNonEquiJoin(right, strategy.getRightAction(), false);

        // Step 2: Create the join itself.
        HazelcastRelOptCluster cluster = OptUtils.getCluster(left);
        DistributionTraitDef distributionTraitDef = cluster.getDistributionTraitDef();

        DistributionTrait distribution = strategy.getResultDistribution() == JoinDistribution.REPLICATED
            ?  distributionTraitDef.getTraitReplicated() : distributionTraitDef.getTraitPartitionedUnknown();

        RelCollation collation = OptUtils.getCollation(leftCollocated);

        RelTraitSet traitSet = OptUtils.toPhysicalConvention(
            cluster.getPlanner().emptyTraitSet(),
            distribution,
            collation
        );

        // Since this is not an equi-join, there should be no left or right keys.
        assert logicalJoin.getLeftKeys().isEmpty();
        assert logicalJoin.getRightKeys().isEmpty();

        return new NestedLoopJoinPhysicalRel(
            cluster,
            traitSet,
            leftCollocated,
            rightCollocated,
            logicalJoin.getCondition(),
            logicalJoin.getJoinType(),
            logicalJoin.getLeftKeys(),
            logicalJoin.getRightKeys()
        );
    }

    private RelNode createCollocatedInputNonEquiJoin(RelNode input, JoinCollocationAction action, boolean left) {
        if (action == JoinCollocationAction.NONE) {
            if (left) {
                return input;
            }

            // TODO: At the moment we pessimistically add materializer here in order to avoid reset() call on operators
            //  which either do not support reset() or their reset is too expensive.
            //  This decision is unfortunate:
            //  1) Some rels which do support reset does not need it, so we end up with not optimal plan
            //  2) After the optimization is finished, we may optionally remove that step if we pull upstream resetability
            //     But the problem is that by this point we already made invalid optimizer decision and possibly chosen
            //     the wrong plan. E.g. the plan A might be refused due to additional costs of materialization, and the plan
            //     B is chosen. But if we had a chance to remove the materialization from A beforehand, it would have won.
            //     How to deal with it?
            //  3) One possible solution is to set resetability flag when a PhysicalRel is created based on the
            //     resetability of its inputs.
            return new MaterializedInputPhysicalRel(input.getCluster(), input.getTraitSet(), input);
        } else {
            // Only BROADCAST actions are expected for non-equi joins.
            assert action == JoinCollocationAction.BROADCAST;

            // Replicated input should not be broadcast further.
            assert OptUtils.getDistribution(input).getType() != DistributionType.REPLICATED;

            HazelcastRelOptCluster cluster = OptUtils.getCluster(input);
            RelTraitSet traitSet = OptUtils.toPhysicalConvention(
                cluster.getPlanner().emptyTraitSet(),
                cluster.getDistributionTraitDef().getTraitReplicated()
            );

            // Step 1: Broadcast data.
            BroadcastExchangePhysicalRel exchange = new BroadcastExchangePhysicalRel(cluster, traitSet, input);

            if (left) {
                return exchange;
            }

            // Step 2: Accumulate data in an in-memory table for further nested loop join.
            // TODO: Shouldn't materializer preserve collation?
            return new MaterializedInputPhysicalRel(cluster, traitSet, exchange);
        }
    }

    /**
     * Get join distribution for the given input distribution for non-equi join.
     *
     * Since Hazelcast spreads data by hash only, for non-equi join we distinguish only between replicated and not replicated
     * cases.
     *
     * @param inputDist Input distribution.
     * @return Join distribution.
     */
    private static JoinDistribution joinDistributionForNonEquiJoin(DistributionTrait inputDist) {
        return inputDist.getType() == DistributionType.REPLICATED ? JoinDistribution.REPLICATED : JoinDistribution.RANDOM;
    }

    /**
     * Create transformations for equi join.
     *
     * @param type Join type.
     * @param logicalJoin Logical join.
     * @param left Left input.
     * @param right Right input.
     * @return Transformations.
     */
    @SuppressWarnings({"checkstyle:MethodLength", "checkstyle:CyclomaticComplexity"})
    private List<RelNode> createTransformsEquiJoin(
        JoinType type,
        JoinLogicalRel logicalJoin,
        RelNode left,
        RelNode right
    ) {
        int memberCount = OptUtils.getDistributionDef(left).getMemberCount();

        DistributionTrait leftDist = OptUtils.getDistribution(left);
        DistributionTrait rightDist = OptUtils.getDistribution(right);

        InputDistribution leftInputDist = prepareInputDistribution(leftDist, logicalJoin.getLeftKeys());
        InputDistribution rightInputDist = prepareInputDistribution(rightDist, logicalJoin.getRightKeys());

        boolean partitionedCollocated = leftInputDist.isPartitioned() && rightInputDist.isPartitioned()
            && leftInputDist.isPartitionedCollocated(rightInputDist.getDistributionJoinKeyPositions());

        List<RelNode> res = new ArrayList<>(1);

        if (partitionedCollocated) {
            // Both inputs are collocated.
            // E.g. aDist={a1, a2}, bDist={b1, b2}, cond={a1=b1, a2=b2}.
            List<JoinCollocationStrategy> strategies = JoinCollocationStrategy.resolve(
                memberCount,
                type,
                JoinConditionType.EQUI,
                JoinDistribution.PARTITIONED,
                JoinDistribution.PARTITIONED
            );

            for (JoinCollocationStrategy strategy : strategies) {
                RelNode node = createTransformEquiJoin(
                    type,
                    logicalJoin,
                    left,
                    right,
                    strategy,
                    leftInputDist.getDistributionJoinKeyPositions()
                );

                res.add(node);
            }
        } else if (leftInputDist.isPartitioned() || rightInputDist.isPartitioned()) {
            // At least one input is partitioned, but together they are not collocated. In this case we explore up to two
            // join strategies:
            // 1) Leave left input where it is and move the right one to it
            // 2) The opposite: leave right input where it is and move the left one to it
            if (leftInputDist.isPartitioned()) {
                List<JoinCollocationStrategy> strategies = JoinCollocationStrategy.resolve(
                    memberCount,
                    type,
                    JoinConditionType.EQUI,
                    JoinDistribution.PARTITIONED,
                    rightInputDist.getDistributionPartitionedAsRandom()
                );

                for (JoinCollocationStrategy strategy : strategies) {
                    RelNode node = createTransformEquiJoin(
                        type,
                        logicalJoin,
                        left,
                        right,
                        strategy,
                        leftInputDist.getDistributionJoinKeyPositions()
                    );

                    res.add(node);
                }
            }

            if (rightInputDist.isPartitioned()) {
                List<JoinCollocationStrategy> strategies = JoinCollocationStrategy.resolve(
                    memberCount,
                    type,
                    JoinConditionType.EQUI,
                    leftInputDist.getDistributionPartitionedAsRandom(),
                    JoinDistribution.PARTITIONED
                );

                for (JoinCollocationStrategy strategy : strategies) {
                    RelNode node = createTransformEquiJoin(
                        type,
                        logicalJoin,
                        left,
                        right,
                        strategy,
                        rightInputDist.getDistributionJoinKeyPositions()
                    );

                    res.add(node);
                }
            }
        } else {
            // Neither inputs are PARTITIONED with known distribution fields.
            List<JoinCollocationStrategy> strategies = JoinCollocationStrategy.resolve(
                memberCount,
                type,
                JoinConditionType.EQUI,
                leftInputDist.getDistribution(),
                rightInputDist.getDistribution()
            );

            // We will do hashing on all equi-join fields, since there is no better option.
            List<Integer> distributionJoinKeyPositions = new ArrayList<>();

            for (int i = 0; i < logicalJoin.getLeftKeys().size(); i++) {
                distributionJoinKeyPositions.add(i);
            }

            for (JoinCollocationStrategy strategy : strategies) {
                RelNode node = createTransformEquiJoin(
                    type,
                    logicalJoin,
                    left,
                    right,
                    strategy,
                    distributionJoinKeyPositions
                );

                res.add(node);
            }
        }

        return res;
    }

    private RelNode createTransformEquiJoin(
        JoinType type,
        JoinLogicalRel logicalJoin,
        RelNode left,
        RelNode right,
        JoinCollocationStrategy strategy,
        List<Integer> hashFields
    ) {
        // Step 1: Force collocation on inputs.
        List<Integer> leftHashKeys = distributionJoinKeys(logicalJoin.getLeftKeys(), hashFields);
        List<Integer> rightHashKeys = distributionJoinKeys(logicalJoin.getRightKeys(), hashFields);

        RelNode leftCollocated = createCollocatedInputEquiJoin(left, strategy.getLeftAction(), leftHashKeys);
        RelNode rightCollocated = createCollocatedInputEquiJoin(right, strategy.getRightAction(), rightHashKeys);

        // Step 2: Create the join.
        HazelcastRelOptCluster cluster = OptUtils.getCluster(left);

        DistributionTrait distribution = prepareDistributionEquiJoin(
            cluster.getDistributionTraitDef(),
            strategy.getResultDistribution(),
            leftHashKeys,
            rightHashKeys,
            leftCollocated.getRowType().getFieldCount()
        );

        RelCollation collation = OptUtils.getCollation(leftCollocated);

        RelTraitSet traitSet = OptUtils.toPhysicalConvention(
            cluster.getPlanner().emptyTraitSet(),
            distribution,
            collation
        );

        return new HashJoinPhysicalRel(
            cluster,
            traitSet,
            leftCollocated,
            rightCollocated,
            logicalJoin.getCondition(),
            logicalJoin.getJoinType(),
            logicalJoin.getLeftKeys(),
            logicalJoin.getRightKeys(),
            leftHashKeys,
            rightHashKeys
        );
    }

    /**
     * Get positions of join keys which should be used for hashing.
     *
     * @param allJoinKeys All join keys.
     * @param distributionJoinKeyPositions Positions of join keys which are used for distribution.
     * @return Hash keys.
     */
    private static List<Integer> distributionJoinKeys(List<Integer> allJoinKeys, List<Integer> distributionJoinKeyPositions) {
        List<Integer> res = new ArrayList<>(distributionJoinKeyPositions.size());

        for (Integer distributionJoinKeyPosition : distributionJoinKeyPositions) {
            res.add(allJoinKeys.get(distributionJoinKeyPosition));
        }

        return res;
    }

    /**
     * Prepare distribution trait for the equi-join relation. Since the resulting relation is collocated on both left join keys
     * and right join keys, we create composite distribution trait, which takes in count both.
     *
     * @param joinDistribution Join distribution.
     * @param leftJoinKeys Left join keys.
     * @param rightJoinKeys Right join keys.
     * @param leftFieldCount Number of fields of the left input.
     * @return Distribution trait.
     */
    private static DistributionTrait prepareDistributionEquiJoin(
        DistributionTraitDef distributionTraitDef,
        JoinDistribution joinDistribution,
        List<Integer> leftJoinKeys,
        List<Integer> rightJoinKeys,
        int leftFieldCount
    ) {
        switch (joinDistribution) {
            case REPLICATED:
                return distributionTraitDef.getTraitReplicated();

            case RANDOM:
                return distributionTraitDef.getTraitPartitionedUnknown();

            default:
                assert joinDistribution == JoinDistribution.PARTITIONED;

                List<Integer> leftGroups = prepareDistributionFieldsEquiJoin(leftJoinKeys, 0);
                List<Integer> rightGroup = prepareDistributionFieldsEquiJoin(rightJoinKeys, leftFieldCount);

                List<List<Integer>> groups = new ArrayList<>(2);
                groups.add(leftGroups);
                groups.add(rightGroup);

                return distributionTraitDef.createPartitionedTrait(groups);
        }
    }

    /**
     * Convert join keys to distribution fields.
     *
     * @param joinKeys Join keys.
     * @param offset Offset. Zero for the left input, [num of fields on the left input] for the right input.
     * @return Distribution fields.
     */
    private static List<Integer> prepareDistributionFieldsEquiJoin(List<Integer> joinKeys, int offset) {
        List<Integer> res = new ArrayList<>(joinKeys.size());

        joinKeys.forEach((fieldIndex) -> res.add(fieldIndex + offset));

        return res;
    }

    private RelNode createCollocatedInputEquiJoin(
        RelNode input,
        JoinCollocationAction action,
        List<Integer> hashFields
    ) {
        if (action == JoinCollocationAction.NONE) {
            // No transformation for input is needed.
            return input;
        }

        HazelcastRelOptCluster cluster = OptUtils.getCluster(input);
        DistributionTraitDef distributionTraitDef = cluster.getDistributionTraitDef();

        DistributionTrait partitionedDistribution = distributionTraitDef.createPartitionedTrait(
            Collections.singletonList(prepareDistributionFieldsEquiJoin(hashFields, 0))
        );

        if (action == JoinCollocationAction.REPLICATED_HASH) {
            // Hash replicated input to convert it to partitioned form.
            // Distribution is changed to PARTITIONED on hash columns. Collation is lost.
            assert OptUtils.getDistribution(input).getType() == DistributionType.REPLICATED;

            RelTraitSet replicatedToPartitionedTraitSet = OptUtils.toPhysicalConvention(
                cluster.getPlanner().emptyTraitSet(),
                partitionedDistribution,
                OptUtils.getCollation(input)
            );

            return new ReplicatedToDistributedPhysicalRel(
                cluster,
                replicatedToPartitionedTraitSet,
                input,
                hashFields
            );
        } else if (action == JoinCollocationAction.BROADCAST) {
            // Do broadcast. Distribution is changed to REPLICATED. Collation is lost.
            RelTraitSet broadcastTraitSet = OptUtils.toPhysicalConvention(
                cluster.getPlanner().emptyTraitSet(),
                distributionTraitDef.getTraitReplicated()
            );

            return new BroadcastExchangePhysicalRel(
                cluster,
                broadcastTraitSet,
                input
            );
        } else {
            // Do unicast. Distribution is changed to PARTITIONED on hash columns. Collation is lost.
            assert action == JoinCollocationAction.UNICAST;

            RelTraitSet unicastTraitSet = OptUtils.toPhysicalConvention(
                cluster.getPlanner().emptyTraitSet(),
                partitionedDistribution
            );

            return new UnicastExchangePhysicalRel(
                cluster,
                unicastTraitSet,
                input,
                hashFields
            );
        }
    }

    /**
     * Prepare distribution of a join side based on join condition. This function is a key to check whether two inputs are
     * collocated.
     * <p>
     * Consider the join "A join B on A.a1 = B.b1 AND A.a2 = B.b2".
     * 1) If A is distributed by [a1], then the result is PARTITIONED(a1)
     * 2) If A is distributed by [a2], then the result is PARTITIONED(a2)
     * 3) If A is distributed by [a1, a2], then the result is PARTITIONED(a1, a2)
     * 4) If A is distributed by [a3], then the result is RANDOM
     * 5) If A is distributed by [a2, a3], then the result is RANDOM
     * 6) If A is a replicated map, then the result is REPLICATED
     *
     * @param dist Original rel distribution.
     * @param joinKeys Join keys.
     * @return Input distribution for join.
     */
    private static InputDistribution prepareInputDistribution(DistributionTrait dist, List<Integer> joinKeys) {
        DistributionType type = dist.getType();

        if (type == DistributionType.REPLICATED) {
            return new InputDistribution(JoinDistribution.REPLICATED, null);
        }

        // Singleton is treated as a special case of partitioned distribution.
        if (type == DistributionType.ROOT) {
            return new InputDistribution(JoinDistribution.RANDOM, null);
        }

        // Deal with partitioned distribution. Try to find the matching group.
        assert type == DistributionType.PARTITIONED;

        for (List<Integer> fields : dist.getFieldGroups()) {
            List<Integer> mappedJoinKeys = mapPartitionedDistributionKeys(joinKeys, fields);

            if (mappedJoinKeys != null) {
                return new InputDistribution(JoinDistribution.PARTITIONED, mappedJoinKeys);
            }
        }

        // Failed to map distribution. Treat as random.
        return new InputDistribution(JoinDistribution.RANDOM, null);
    }

    /**
     * Create a BitSet of join keys which are part of the distribution fields. If at least one distribution field is not
     * present among join keys, then return {@code null}, which means that the distribution is lost during join (i.e. we
     * fallback to RANDOM distribution).
     * <p>
     * Consider the join "A join B on A.a1 = B.b1 AND A.a2 = B.b2 AND A.a3 = B.b3". If A is distributed by a2, then:
     * 1) joinKeys = [0, 1, 2] / 0 stands for a1, 1 stands for a2, 2 stands for a3
     * 2) fields = [1]
     * 3) result: [1], which means that the field a2 is used for distribution.
     *
     * @param joinKeys Join keys.
     * @param fields Distribution fields.
     * @return Join keys BitSet.
     */
    private static List<Integer> mapPartitionedDistributionKeys(List<Integer> joinKeys, List<Integer> fields) {
        List<Integer> res = new ArrayList<>(joinKeys.size());

        for (Integer field : fields) {
            int joinKeyIndex = joinKeys.indexOf(field);

            if (joinKeyIndex == -1) {
                return null;
            }

            res.add(joinKeyIndex);
        }

        return res;
    }

    private static JoinType getJoinType(JoinRelType calciteType) {
        switch (calciteType) {
            case INNER:
                return JoinType.INNER;

            case LEFT:
                return JoinType.OUTER;

            case FULL:
                return JoinType.FULL;

            case SEMI:
                return JoinType.SEMI;

            default:
                // TODO: Handle right join through transposition: r RIGHT JOIN s == s LEFT JOIN r.
                return null;
        }
    }

    /**
     * Distribution of input for the given join.
     */
    private static final class InputDistribution {
        /** Distribution which should be used for join strategy selection. */
        private final JoinDistribution distribution;

        /** Positions of join keys which are used in distribution. */
        private final List<Integer> distributionJoinKeyPositions;

        private InputDistribution(JoinDistribution distribution, List<Integer> distributionJoinKeyPositions) {
            this.distribution = distribution;
            this.distributionJoinKeyPositions = distributionJoinKeyPositions;
        }

        public JoinDistribution getDistribution() {
            return distribution;
        }

        /**
         * @return Join distribution with PARTITIONED downgraded to RANDOM.
         */
        public JoinDistribution getDistributionPartitionedAsRandom() {
            return isPartitioned() ? JoinDistribution.RANDOM : distribution;
        }

        public List<Integer> getDistributionJoinKeyPositions() {
            return distributionJoinKeyPositions;
        }

        public boolean isPartitioned() {
            return distribution == JoinDistribution.PARTITIONED;
        }

        /**
         * Check if two partitioned inputs are collocated.
         *
         * @param otherDistributionJoinKeyPositions Positions of distribution join keys of the other relation.
         * @return {@code True} if collocated.
         */
        public boolean isPartitionedCollocated(List<Integer> otherDistributionJoinKeyPositions) {
            return distributionJoinKeyPositions.equals(otherDistributionJoinKeyPositions);
        }
    }
}
