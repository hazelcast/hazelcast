package com.hazelcast.sql.impl.calcite.physical.distribution;

import com.hazelcast.sql.impl.calcite.physical.rel.PhysicalRel;
import com.hazelcast.sql.impl.calcite.physical.rel.SingletonExchangePhysicalRel;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;

public class PhysicalDistributionTraitDef extends RelTraitDef<PhysicalDistributionTrait> {
    public static final PhysicalDistributionTraitDef INSTANCE = new PhysicalDistributionTraitDef();

    @Override
    public Class<PhysicalDistributionTrait> getTraitClass() {
        return PhysicalDistributionTrait.class;
    }

    @Override
    public String getSimpleName() {
        return getClass().getSimpleName();
    }

    @Override
    public RelNode convert(
        RelOptPlanner planner,
        RelNode rel,
        PhysicalDistributionTrait targetTrait,
        boolean allowInfiniteCostConverters
    ) {
        PhysicalDistributionTrait currentTrait = rel.getTraitSet().getTrait(PhysicalDistributionTraitDef.INSTANCE);

        if (currentTrait.equals(targetTrait))
            return rel;

        if (currentTrait.equals(PhysicalDistributionTrait.ANY) && !(rel instanceof RelSubset) )
            return null;

        if (rel.getConvention() != PhysicalRel.HAZELCAST_PHYSICAL)
            return null;

        switch (targetTrait.getType()){
            case SINGLETON:
                return new SingletonExchangePhysicalRel(
                    rel.getCluster(),
                    planner.emptyTraitSet().plus(PhysicalRel.HAZELCAST_PHYSICAL).plus(targetTrait),
                    rel
                );

            case ANY:
                return rel;

            default:
                return null;
        }
    }

    @Override
    public boolean canConvert(
        RelOptPlanner planner,
        PhysicalDistributionTrait fromTrait,
        PhysicalDistributionTrait toTrait
    ) {
        return true;
    }

    @Override
    public PhysicalDistributionTrait getDefault() {
        return PhysicalDistributionTrait.ANY;
    }
}
