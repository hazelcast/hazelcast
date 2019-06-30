package com.hazelcast.sql.impl.calcite.physical.distribution;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;

public class HazelcastDistributionTrait implements RelTrait {

    public static final HazelcastDistributionTrait SINGLETON = new HazelcastDistributionTrait(HazelcastDistributionType.SINGLETON);
    public static final HazelcastDistributionTrait ANY = new HazelcastDistributionTrait(HazelcastDistributionType.ANY);

    /** Distribution type. */
    private final HazelcastDistributionType type;

    public HazelcastDistributionTrait(HazelcastDistributionType type) {
        this.type = type;
    }

    public HazelcastDistributionType getType() {
        return type;
    }

    @Override
    public RelTraitDef getTraitDef() {
        return HazelcastDistributionTraitDef.INSTANCE;
    }

    @Override
    public boolean satisfies(RelTrait targetTrait) {
        if (targetTrait instanceof HazelcastDistributionTrait) {
            HazelcastDistributionType targetType = ((HazelcastDistributionTrait)targetTrait).getType();

            if (targetType == HazelcastDistributionType.ANY)
                return true;
        }

        return this.equals(targetTrait);
    }

    @Override
    public void register(RelOptPlanner planner) {
        // No-op.
    }

    @Override
    public String toString() {
        return type.name();
    }
}
