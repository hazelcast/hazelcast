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

package com.hazelcast.sql.impl.calcite.physical.rule;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.calcite.HazelcastConventions;
import com.hazelcast.sql.impl.calcite.RuleUtils;
import com.hazelcast.sql.impl.calcite.logical.rel.JoinLogicalRel;
import com.hazelcast.sql.impl.calcite.physical.distribution.DistributionField;
import com.hazelcast.sql.impl.calcite.physical.distribution.DistributionTrait;
import com.hazelcast.sql.impl.calcite.physical.rel.join.CollocatedJoinPhysicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.sql.impl.calcite.physical.distribution.DistributionTrait.DISTRIBUTED_DIST;
import static com.hazelcast.sql.impl.calcite.physical.distribution.DistributionTrait.REPLICATED_DIST;
import static com.hazelcast.sql.impl.calcite.physical.distribution.DistributionTrait.SINGLETON_DIST;
import static com.hazelcast.sql.impl.calcite.physical.distribution.DistributionType.DISTRIBUTED;

/**
 * The rule that tries to created a collocated join.
 */
public final class CollocatedJoinPhysicalRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new CollocatedJoinPhysicalRule();

    private CollocatedJoinPhysicalRule() {
        super(
            RuleUtils.single(JoinLogicalRel.class, HazelcastConventions.LOGICAL),
            CollocatedJoinPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        JoinLogicalRel logicalJoin = call.rel(0);

        RelNode convertedLeft = RuleUtils.toPhysicalInput(logicalJoin.getLeft());
        RelNode convertedRight = RuleUtils.toPhysicalInput(logicalJoin.getRight());

        Collection<RelNode> leftCandidates = RuleUtils.getPhysicalRelsFromSubset(convertedLeft);
        Collection<RelNode> rightCandidates = RuleUtils.getPhysicalRelsFromSubset(convertedRight);

        List<RelNode> transforms = new ArrayList<>(1);

        if (!leftCandidates.isEmpty() && !rightCandidates.isEmpty()) {
            for (RelNode leftCandidate : leftCandidates) {
                for (RelNode rightCandidate : rightCandidates) {
                    RelNode transform = tryCreateTransform(logicalJoin, leftCandidate, rightCandidate);

                    if (transform != null) {
                        transforms.add(transform);
                    }
                }
            }
        }

        for (RelNode transform : transforms) {
            call.transformTo(transform);
        }

        // TODO: Iff this is an equijoin!
        // TODO: Iff top-level conjunction on collocated columns are present
        // TODO: Then a collocated join is applicable
    }

    /**
     * Try creating collocated physical join for the given logical join and physical inputs.
     *
     * @param logicalJoin Original logical join.
     * @param left Physical left input.
     * @param right Physical right input.
     * @return Collocated join or {@code null} if collocation is not possible.
     */
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private CollocatedJoinPhysicalRel tryCreateTransform(JoinLogicalRel logicalJoin, RelNode left, RelNode right) {
        JoinRelType joinType = logicalJoin.getJoinType();

        if (joinType == JoinRelType.FULL) {
            // TODO: Implement. Through UNION?
            throw new HazelcastSqlException(-1, "FULL JOIN is not supported at the moment.");
        } else if (joinType == JoinRelType.RIGHT) {
            // TODO: Investigate how right joins are handled in Calcite. Are they converted to left joins or not?
            // TODO: If not, is it possible to convert a RIGHT JOIN to LEFT JOIN in all cases or not?
            throw new HazelcastSqlException(-1, "RIGHT JOIN is not supported at the moment.");
        }

        DistributionTrait leftDist = RuleUtils.getDistribution(left);
        DistributionTrait rightDist = RuleUtils.getDistribution(right);

        // If two REPLICATED or SINGLETON sources are joined, the operation is always collocated.
        if (leftDist.isComplete() && rightDist.isComplete()) {
            return tryCreateTransformForCompleteInputs(logicalJoin, left, right);
        }

        // Singletons are not compatible with any other input types.
        if (leftDist == SINGLETON_DIST || rightDist == SINGLETON_DIST) {
            return null;
        }

        // At least one of inputs are distributed at this point.
        switch (joinType) {
            case INNER:
                // In case of inner join if any side is REPLICATED, we are safe to do a collocated join.
                if (leftDist == REPLICATED_DIST || rightDist == REPLICATED_DIST) {
                    // Use distribution of the other input.
                    DistributionTrait joinDist = leftDist == REPLICATED_DIST ? rightDist : leftDist;

                    return createCollocatedJoin(logicalJoin, joinDist, left, right);
                }

                // At this point only distributed inputs remained. Check if at least one collocated pair of columns
                // are found in their join conditions.
                return tryCreateTransformForDistributedInputs(logicalJoin, left, right);

            case LEFT:
                // "replicated LEFT JOIN distributed" is a tricky case because we need to check that a tuple from
                // the replicated input has no matching tuples from the partitioned input on *ALL* data nodes. Hence,
                // this is not a collocated operation.
                if (leftDist == REPLICATED_DIST) {
                    break;
                }

                // To contrast, "partitioned LEFT JOIN replicated" qualifies for collocated join because any tuple of
                // partitioned input exists only on a single node, so local check is enough.
                if (rightDist == REPLICATED_DIST) {
                    return createCollocatedJoin(logicalJoin, leftDist, left, right);
                }

                // Only distributed inputs remained. Provided that the left input is distributed and every tuple from
                // that input is located on a single node, collocated condition is verified the same way as for INNER.
                return tryCreateTransformForDistributedInputs(logicalJoin, left, right);

            default:
                break;
        }

        return null;
    }

    /**
     * Create collocated join for two complete inputs.
     *
     * @param logicalJoin Logical join.
     * @param left Physical left input.
     * @param right Physical right input.
     * @return Collocated join, never null.
     */
    private CollocatedJoinPhysicalRel tryCreateTransformForCompleteInputs(
        JoinLogicalRel logicalJoin,
        RelNode left,
        RelNode right
    ) {
        DistributionTrait leftDist = RuleUtils.getDistribution(left);
        DistributionTrait rightDist = RuleUtils.getDistribution(right);

        // Distribution of the result is SINGLETON if at least one input is singleton.
        DistributionTrait joinDist = leftDist == SINGLETON_DIST || rightDist == SINGLETON_DIST ? SINGLETON_DIST : REPLICATED_DIST;

        return createCollocatedJoin(logicalJoin, joinDist, left, right);
    }

    /**
     * Try creating a collocated join for distributed inputs. This is only possible in case at least one equi-join
     * condition is performed on the distribution keys.
     *
     * @param logicalJoin Logical join.
     * @param left Left physical input.
     * @param right Right physical input.
     * @return Collocated join or {@code null} if inputs are not collocated.
     */
    private CollocatedJoinPhysicalRel tryCreateTransformForDistributedInputs(
        JoinLogicalRel logicalJoin,
        RelNode left,
        RelNode right
    ) {
        DistributionTrait leftDist = RuleUtils.getDistribution(left);
        DistributionTrait rightDist = RuleUtils.getDistribution(right);

        assert leftDist.getType() == DISTRIBUTED;
        assert rightDist.getType() == DISTRIBUTED;

        // If we do not have information about distribution columns of at least one input, it is impossible to
        // do the collocated join.
        if (leftDist == DISTRIBUTED_DIST || rightDist == DISTRIBUTED_DIST) {
            return null;
        }

        List<Integer> leftKeys = logicalJoin.getLeftKeys();
        List<Integer> rightKeys = logicalJoin.getRightKeys();

        // No equality conditions - collocation is not possible.
        if (leftKeys.isEmpty()) {
            return null;
        }

        List<Integer> leftDistFields = getDistributionFields(leftDist);
        List<Integer> rightDistFields = getDistributionFields(rightDist);

        List<DistributionField> joinDistFields = new ArrayList<>(1);

        // Iterate over distribution fields of inputs.
        for (int i = 0; i < Math.min(leftDistFields.size(), rightDistFields.size()); i++) {
            Integer leftDistField = leftDistFields.get(i);
            Integer rightDistField = rightDistFields.get(i);

            // Check if desired distribution fields are present in join keys. If distribution field of the relevant
            // input is not in join key, or relevant join keys of left and right input are not in the same equality
            // condition, then there is no collocation and we stop the loop.
            int leftKeyIdx = leftKeys.indexOf(leftDistField);
            int rightKeyIdx = rightKeys.indexOf(rightDistField);

            if (leftKeyIdx == -1 || leftKeyIdx != rightKeyIdx) {
                break;
            }

            // New join may use either distribution prefix of a left input, or a right input, but not both.
            // We use the left one for the sake of simplicity.

            // TODO: The decision to use columns of the left input appears to be dangerous. Consider the following SQL:
            // TODO: "SELECT b FROM (SELECT A.a, B.b FROM A JOIN B on A.a = B.b)"
            // TODO: The inner query will return partition distribution of {A.a}, but parent projection will destroy it.
            // TODO: BUT! Should we choose the distribution of {B.b}, then parent projection will retain it, what may
            // TODO: allow further optimizations of the downstream operators!

            // TODO: BOTTOM LINE: Looks like it should be possible to maintain equivalent distribution fields! In this
            // TODO: example it will be {A.a}{B.b} (not to confuse with {A.a, B.b}).
            DistributionField joinDistField = new DistributionField(leftDistField);

            joinDistFields.add(joinDistField);
        }

        // If none distribution fields were created, then there is no collocation.
        if (joinDistFields.isEmpty()) {
            return null;
        }

        // Otherwise create collocated join with the given distribution fields.
        DistributionTrait joinDist = DistributionTrait.Builder.ofType(DISTRIBUTED).addFieldGroup(joinDistFields).build();

        return createCollocatedJoin(logicalJoin, joinDist, left, right);
    }

    /**
     * Get the prefix of distribution fields for the given input, until the first nested field is found.
     *
     * @param dist Distribution.
     * @return Field indexes.
     */
    private static List<Integer> getDistributionFields(DistributionTrait dist) {
        assert dist.getType() == DISTRIBUTED;

        // TODO: FIx me as a part of join refactoring.
        List<DistributionField> distFields = dist.getFieldGroups().get(0);

        assert !distFields.isEmpty();

        List<Integer> res = new ArrayList<>(distFields.size());

        for (DistributionField distField : distFields) {
            if (distField.getNestedField() != null) {
                break;
            }

            res.add(distField.getIndex());
        }

        return res;
    }

    /**
     * Create collocated join.
     *
     * @param logicalJoin Logical join.
     * @param joinDist Distribution for the join.
     * @param left Left physical input.
     * @param right Right physical input.
     * @return Collocated join.
     */
    private CollocatedJoinPhysicalRel createCollocatedJoin(
        JoinLogicalRel logicalJoin,
        DistributionTrait joinDist,
        RelNode left,
        RelNode right
    ) {
        RelTraitSet traitSet = RuleUtils.toPhysicalConvention(logicalJoin.getTraitSet(), joinDist);

        // TODO: Understand how to handle collation properly. It depends on the physical join method:
        // TODO: 1) Nested loops: use collation of the fields of the outer (left) input
        // TODO: 2) Merge join: use combine collation of the fields of both inputs
        // TODO: 3) Hash join: probe input collation is destroyed, but collation of the other input could be used.

        return new CollocatedJoinPhysicalRel(
            logicalJoin.getCluster(),
            traitSet,
            left,
            right,
            logicalJoin.getCondition(),
            logicalJoin.getJoinType(),
            logicalJoin.getLeftKeys(),
            logicalJoin.getRightKeys()
        );
    }
}
