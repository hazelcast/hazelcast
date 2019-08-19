package com.hazelcast.sql.impl.calcite;

import com.hazelcast.sql.impl.calcite.physical.distribution.PhysicalDistributionTrait;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;

import java.util.HashSet;
import java.util.Set;

import static org.apache.calcite.plan.RelOptRule.convert;

/**
 * Static utility classes for rules.
 */
public class RuleUtils {
    /**
     * Get operand matching a single node.
     *
     * @param cls Node class.
     * @param convention Convention.
     * @return Operand.
     */
    public static <R extends RelNode> RelOptRuleOperand single(Class<R> cls, Convention convention) {
        return RelOptRule.operand(cls, convention, RelOptRule.any());
    }

    /**
     * Get operand matching a node with specific child node.
     *
     * @param cls Node class.
     * @param childCls Child node class.
     * @param convention Convention.
     * @return Operand.
     */
    public static <R1 extends RelNode, R2 extends RelNode> RelOptRuleOperand parentChild(Class<R1> cls,
        Class<R2> childCls, Convention convention) {
        RelOptRuleOperand childOperand = RelOptRule.operand(childCls, RelOptRule.any());

        return RelOptRule.operand(cls, convention, RelOptRule.some(childOperand));
    }

    /**
     * Add a single trait to the trait set.
     *
     * @param traitSet Original trait set.
     * @param trait Trait to add.
     * @return Resulting trait set.
     */
    public static RelTraitSet traitPlus(RelTraitSet traitSet, RelTrait trait) {
        return traitSet.plus(trait).simplify();
    }

    /**
     * Add two traits to the trait set.
     *
     * @param traitSet Original trait set.
     * @param trait1 Trait to add.
     * @param trait2 Trait to add.
     * @return Resulting trait set.
     */
    public static RelTraitSet traitPlus(RelTraitSet traitSet, RelTrait trait1, RelTrait trait2) {
        return traitSet.plus(trait1).plus(trait2).simplify();
    }

    /**
     * Add three traits to the trait set.
     *
     * @param traitSet Original trait set.
     * @param trait1 Trait to add.
     * @param trait2 Trait to add.
     * @param trait3 Trait to add.
     * @return Resulting trait set.
     */
    public static RelTraitSet traitPlus(RelTraitSet traitSet, RelTrait trait1, RelTrait trait2, RelTrait trait3) {
        return traitSet.plus(trait1).plus(trait2).plus(trait3).simplify();
    }

    /**
     * Convert the given trait set to logical convention.
     *
     * @param traitSet Original trait set.
     * @return New trait set with logical convention.
     */
    public static RelTraitSet toLogicalConvention(RelTraitSet traitSet) {
        return traitPlus(traitSet, HazelcastConventions.LOGICAL);
    }

    /**
     * Convert the given input into logical input.
     *
     * @param input Original input.
     * @return Logical input.
     */
    public static RelNode toLogicalInput(RelNode input) {
        return convert(input, toLogicalConvention(input.getTraitSet()));
    }

    /**
     * Convert the given trait set to physical convention.
     *
     * @param traitSet Original trait set.
     * @return New trait set with physical convention and provided distribution.
     */
    public static RelTraitSet toPhysicalConvention(RelTraitSet traitSet) {
        return traitPlus(traitSet, HazelcastConventions.PHYSICAL);
    }

    /**
     * Convert the given trait set to physical convention.
     *
     * @param traitSet Original trait set.
     * @param distribution Distribution.
     * @return New trait set with physical convention and provided distribution.
     */
    public static RelTraitSet toPhysicalConvention(RelTraitSet traitSet, PhysicalDistributionTrait distribution) {
        return traitPlus(traitSet, HazelcastConventions.PHYSICAL, distribution);
    }

    /**
     * Convert the given input into physical input.
     *
     * @param input Original input.
     * @return Logical input.
     */
    public static RelNode toPhysicalInput(RelNode input) {
        return convert(input, toPhysicalConvention(input.getTraitSet()));
    }

    /**
     * Convert the given input into physical input.
     *
     * @param input Original input.
     * @param distribution Distribution.
     * @return Logical input.
     */
    public static RelNode toPhysicalInput(RelNode input, PhysicalDistributionTrait distribution) {
        return convert(input, toPhysicalConvention(input.getTraitSet(), distribution));
    }

    /**
     * @param rel Node.
     * @return {@code True} if the given node is physical node.
     */
    public static boolean isPhysical(RelNode rel) {
        return rel.getTraitSet().getTrait(ConventionTraitDef.INSTANCE).equals(HazelcastConventions.PHYSICAL);
    }

    /**
     * Get combinations of trait sets of physical relations of the given subset.
     *
     * @param subset Subset.
     * @return Trait sets.
     */
    public static Set<RelTraitSet> getPhysicalTraitSets(RelSubset subset) {
        Set<RelTraitSet> traitSets = new HashSet<>();

        for (RelNode rel : subset.getRelList()) {
            if (!isPhysical(rel))
                continue;

            traitSets.add(rel.getTraitSet());
        }

        return traitSets;
    }

    private RuleUtils() {
        // No-op.
    }
}
