/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.physical.rule.join;

import com.hazelcast.sql.impl.calcite.HazelcastConventions;
import com.hazelcast.sql.impl.calcite.RuleUtils;
import com.hazelcast.sql.impl.calcite.logical.rel.JoinLogicalRel;
import com.hazelcast.sql.impl.calcite.physical.distribution.DistributionField;
import com.hazelcast.sql.impl.calcite.physical.distribution.DistributionTrait;
import com.hazelcast.sql.impl.calcite.physical.distribution.DistributionType;
import com.hazelcast.sql.impl.calcite.physical.rel.MaterializedInputPhysicalRel;
import com.hazelcast.sql.impl.calcite.physical.rel.ReplicatedToDistributedPhysicalRel;
import com.hazelcast.sql.impl.calcite.physical.rel.exchange.BroadcastExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.physical.rel.exchange.UnicastExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.physical.rel.join.HashJoinPhysicalRel;
import com.hazelcast.sql.impl.calcite.physical.rel.join.NestedLoopJoinPhysicalRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.sql.impl.calcite.physical.distribution.DistributionTrait.DISTRIBUTED_DIST;
import static com.hazelcast.sql.impl.calcite.physical.distribution.DistributionTrait.REPLICATED_DIST;

/**
 * General rule for join processing.
 */
public class JoinPhysicalRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new JoinPhysicalRule();

    private JoinPhysicalRule() {
        super(
            RuleUtils.parentChildChild(JoinLogicalRel.class, RelNode.class, RelNode.class, HazelcastConventions.LOGICAL),
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

        RelNode convertedLeft = RuleUtils.toPhysicalInput(logicalJoin.getLeft());
        RelNode convertedRight = RuleUtils.toPhysicalInput(logicalJoin.getRight());

        Collection<RelNode> leftCandidates = RuleUtils.getPhysicalRelsFromSubset(convertedLeft);
        Collection<RelNode> rightCandidates = RuleUtils.getPhysicalRelsFromSubset(convertedRight);

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
     * Create transformations for equi join.
     *
     * @param type Join type.
     * @param logicalJoin Logical join.
     * @param left Left input.
     * @param right Right input.
     * @return Transformations.
     */
    private List<RelNode> createTransformsEquiJoin(
        JoinType type,
        JoinLogicalRel logicalJoin,
        RelNode left,
        RelNode right
    ) {
        DistributionTrait leftDist = RuleUtils.getDistribution(left);
        DistributionTrait rightDist = RuleUtils.getDistribution(right);

        InputDistribution leftInputDist = prepareInputDistribution(leftDist, logicalJoin.getLeftKeys());
        InputDistribution rightInputDist = prepareInputDistribution(rightDist, logicalJoin.getRightKeys());

        boolean partitionedCollocated = leftInputDist.isPartitioned() && rightInputDist.isPartitioned()
            && leftInputDist.isCollocated(rightInputDist.getDistributionFields());

        List<RelNode> res = new ArrayList<>(1);

        if (partitionedCollocated) {
            // Both inputs are collocated.
            // E.g. aDist={a1, a2}, bDist={b1, b2}, cond={a1=b1, a2=b2}.
            List<JoinCollocationStrategy> strategies = JoinCollocationStrategy.resolve(
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
                    leftInputDist.getDistributionFields()
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
                        leftInputDist.getDistributionFields()
                    );

                    res.add(node);
                }
            }

            if (rightInputDist.isPartitioned()) {
                List<JoinCollocationStrategy> strategies = JoinCollocationStrategy.resolve(
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
                        rightInputDist.getDistributionFields()
                    );

                    res.add(node);
                }
            }
        } else {
            // Neither inputs are PARTITIONED with known distribution fields.
            List<JoinCollocationStrategy> strategies = JoinCollocationStrategy.resolve(
                type,
                JoinConditionType.EQUI,
                leftInputDist.getDistribution(),
                rightInputDist.getDistribution()
            );

            // We will do hashing on all equi-join fields, since there is no better option.
            int joinKeyCount = logicalJoin.getLeftKeys().size();
            BitSet hashFields = new BitSet(joinKeyCount);
            hashFields.set(0, joinKeyCount);

            for (JoinCollocationStrategy strategy : strategies) {
                RelNode node = createTransformEquiJoin(
                    type,
                    logicalJoin,
                    left,
                    right,
                    strategy,
                    hashFields
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
        BitSet hashFields
    ) {
        // Step 1: Force collocation on inputs.
        List<Integer> leftHashKeys = convertHashKeys(logicalJoin.getLeftKeys(), hashFields);
        List<Integer> rightHashKeys = convertHashKeys(logicalJoin.getRightKeys(), hashFields);

        RelNode leftCollocated = createCollocatedInputEquiJoin(left, strategy.getLeftAction(), leftHashKeys);
        RelNode rightCollocated = createCollocatedInputEquiJoin(right, strategy.getRightAction(), rightHashKeys);

        // Step 2: Create the join.
        RelOptCluster cluster = left.getCluster();

        DistributionTrait distribution = prepareDistributionEquiJoin(
            strategy.getResultDistribution(),
            leftHashKeys,
            rightHashKeys,
            leftCollocated.getRowType().getFieldCount()
        );

        RelCollation collation = RuleUtils.getCollation(leftCollocated);

        RelTraitSet traitSet = RuleUtils.toPhysicalConvention(
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

    private static DistributionTrait prepareDistributionEquiJoin(
        JoinDistribution joinDistribution,
        List<Integer> leftJoinKeys,
        List<Integer> rightJoinKeys,
        int leftFieldCount
    ) {
        switch (joinDistribution) {
            case REPLICATED:
                return REPLICATED_DIST;

            case RANDOM:
                return DISTRIBUTED_DIST;

            default:
                assert joinDistribution == JoinDistribution.PARTITIONED;

                List<DistributionField> leftDistFields = toDistributionFields(leftJoinKeys, 0);
                List<DistributionField> rightDistFields = toDistributionFields(rightJoinKeys, leftFieldCount);

                return DistributionTrait.Builder.ofType(DistributionType.DISTRIBUTED)
                    .addFieldGroup(leftDistFields)
                    .addFieldGroup(rightDistFields)
                    .build();
        }
    }

    private static List<DistributionField> toDistributionFields(List<Integer> fields, int offset) {
        List<DistributionField> res = new ArrayList<>(fields.size());

        fields.forEach((fieldIndex) -> {
            res.add(new DistributionField(fieldIndex + offset));
        });

        return res;
    }

    private static List<Integer> convertHashKeys(List<Integer> joinKeys, BitSet mask) {
        List<Integer> res = new ArrayList<>(mask.cardinality());

        mask.stream().forEach((idx) -> {
            res.add(joinKeys.get(idx));
        });

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

        RelOptCluster cluster = input.getCluster();

        DistributionTrait partitionedDistribution = DistributionTrait.Builder.ofType(DistributionType.DISTRIBUTED)
            .addFieldGroup(toDistributionFields(hashFields, 0))
            .build();

        if (action == JoinCollocationAction.REPLICATED_HASH) {
            // Hash replicated input to convert it to partitioned form.
            // Distribution is changed to PARTITIONED on hash columns. Collation is lost.
            assert RuleUtils.getDistribution(input).getType() == DistributionType.REPLICATED;

            RelTraitSet replicatedToPartitionedTraitSet = RuleUtils.toPhysicalConvention(
                cluster.getPlanner().emptyTraitSet(),
                partitionedDistribution,
                RuleUtils.getCollation(input)
            );

            return new ReplicatedToDistributedPhysicalRel(
                cluster,
                replicatedToPartitionedTraitSet,
                input,
                hashFields
            );
        } else if (action == JoinCollocationAction.BROADCAST) {
            // Do broadcast. Distribution is changed to REPLICATED. Collation is lost.
            RelTraitSet broadcastTraitSet = RuleUtils.toPhysicalConvention(
                cluster.getPlanner().emptyTraitSet(),
                REPLICATED_DIST
            );

            return new BroadcastExchangePhysicalRel(
                cluster,
                broadcastTraitSet,
                input
            );
        } else {
            // Do unicast. Distribution is changed to PARTITIONED on hash columns. Collation is lost.
            assert action == JoinCollocationAction.UNICAST;

            RelTraitSet unicastTraitSet = RuleUtils.toPhysicalConvention(
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
        DistributionTrait leftDist = RuleUtils.getDistribution(left);
        DistributionTrait rightDist = RuleUtils.getDistribution(right);

        JoinDistribution leftJoinDist = joinDistributionForNonEquiJoin(leftDist);
        JoinDistribution rightJoinDist = joinDistributionForNonEquiJoin(rightDist);

        List<JoinCollocationStrategy> strategies = JoinCollocationStrategy.resolve(
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
        RelNode leftCollocated = createCollocatedInputNonEquiJoin(left, strategy.getLeftAction());
        RelNode rightCollocated = createCollocatedInputNonEquiJoin(right, strategy.getRightAction());

        // Step 2: Create the join itself.
        RelOptCluster cluster = left.getCluster();

        DistributionTrait distribution = strategy.getResultDistribution() == JoinDistribution.REPLICATED
            ? REPLICATED_DIST : DISTRIBUTED_DIST;

        RelCollation collation = RuleUtils.getCollation(leftCollocated);

        RelTraitSet traitSet = RuleUtils.toPhysicalConvention(
            cluster.getPlanner().emptyTraitSet(),
            distribution,
            collation
        );

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

    private RelNode createCollocatedInputNonEquiJoin(RelNode input, JoinCollocationAction action) {
        if (action == JoinCollocationAction.NONE) {
            return input;
        } else {
            // Only BROADCAST actions are expected for non-equi joins.
            assert action == JoinCollocationAction.BROADCAST;

            // Replicated input should not be broadcast further.
            assert RuleUtils.getDistribution(input).getType() != DistributionType.REPLICATED;

            RelOptCluster cluster = input.getCluster();
            RelTraitSet traitSet = RuleUtils.toPhysicalConvention(cluster.getPlanner().emptyTraitSet(), REPLICATED_DIST);

            // Step 1: Broadcast data.
            BroadcastExchangePhysicalRel exchange = new BroadcastExchangePhysicalRel(cluster, traitSet, input);

            // Step 2: Accumulated data in an in-memory table for further nested loop join.
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

    private static InputDistribution prepareInputDistribution(DistributionTrait dist, List<Integer> joinKeys) {
        DistributionType type = dist.getType();

        if (type == DistributionType.REPLICATED) {
            return new InputDistribution(JoinDistribution.REPLICATED, null);
        }

        // Singleton is treated as a special case of partitioned distribution.
        if (type == DistributionType.SINGLETON) {
            return new InputDistribution(JoinDistribution.RANDOM, null);
        }

        // Deal with partitioned distribution. Try to find the matching group.
        assert type == DistributionType.DISTRIBUTED;

        for (List<DistributionField> fields : dist.getFieldGroups()) {
            BitSet bitSet = mapPartitionedDistributionKeys(joinKeys, fields);

            if (bitSet != null) {
                return new InputDistribution(JoinDistribution.PARTITIONED, bitSet);
            }
        }

        // Failed to map distribution. Treat as random.
        return new InputDistribution(JoinDistribution.RANDOM, null);
    }

    private static BitSet mapPartitionedDistributionKeys(List<Integer> joinKeys, List<DistributionField> fields) {
        BitSet res = new BitSet(joinKeys.size());

        for (DistributionField field : fields) {
            assert field.getNestedField() == null;

            int joinKeyIndex = joinKeys.indexOf(field.getIndex());

            if (joinKeyIndex == -1) {
                return null;
            }

            res.set(joinKeyIndex);
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
        }

        // TODO: Handle right join through transposition: r RIGHT JOIN s == s LEFT JOIN r.
        return null;
    }

    /**
     * Distribution of input for the given join.
     */
    private static final class InputDistribution {
        private final JoinDistribution distribution;
        private final BitSet distributionFields;

        private InputDistribution(JoinDistribution distribution, BitSet distributionFields) {
            this.distribution = distribution;
            this.distributionFields = distributionFields;
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

        public BitSet getDistributionFields() {
            return distributionFields;
        }

        public boolean isPartitioned() {
            return distribution == JoinDistribution.PARTITIONED;
        }

        public boolean isCollocated(BitSet otherDistributionFields) {
            return distributionFields.equals(otherDistributionFields);
        }
    }
}
