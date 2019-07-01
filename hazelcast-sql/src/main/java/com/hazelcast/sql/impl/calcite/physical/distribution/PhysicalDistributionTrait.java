package com.hazelcast.sql.impl.calcite.physical.distribution;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;

public class PhysicalDistributionTrait implements RelTrait {

    public static final PhysicalDistributionTrait PARTITIONED = new PhysicalDistributionTrait(PhysicalDistributionType.PARTITIONED);
    public static final PhysicalDistributionTrait SINGLETON = new PhysicalDistributionTrait(PhysicalDistributionType.SINGLETON);
    public static final PhysicalDistributionTrait ANY = new PhysicalDistributionTrait(PhysicalDistributionType.ANY);

    /** Distribution type. */
    private final PhysicalDistributionType type;

    public PhysicalDistributionTrait(PhysicalDistributionType type) {
        this.type = type;
    }

    public PhysicalDistributionType getType() {
        return type;
    }

    @Override
    public RelTraitDef getTraitDef() {
        return PhysicalDistributionTraitDef.INSTANCE;
    }

    @Override
    public boolean satisfies(RelTrait targetTrait) {
        if (targetTrait instanceof PhysicalDistributionTrait) {
            PhysicalDistributionType targetType = ((PhysicalDistributionTrait)targetTrait).getType();

            if (targetType == PhysicalDistributionType.ANY)
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
