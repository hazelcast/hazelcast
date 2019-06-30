package com.hazelcast.sql.impl.calcite.physical.distribution;

import com.hazelcast.sql.impl.calcite.physical.rel.HazelcastPhysicalRel;
import com.hazelcast.sql.impl.calcite.physical.rel.HazelcastSingletonExchangePhysicalRel;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;

public class HazelcastDistributionTraitDef extends RelTraitDef<HazelcastDistributionTrait> {
    public static final HazelcastDistributionTraitDef INSTANCE = new HazelcastDistributionTraitDef();

    @Override
    public Class<HazelcastDistributionTrait> getTraitClass() {
        return HazelcastDistributionTrait.class;
    }

    @Override
    public String getSimpleName() {
        return getClass().getSimpleName();
    }

    @Override
    public RelNode convert(
        RelOptPlanner planner,
        RelNode rel,
        HazelcastDistributionTrait targetTrait,
        boolean allowInfiniteCostConverters
    ) {
        HazelcastDistributionTrait currentTrait = rel.getTraitSet().getTrait(HazelcastDistributionTraitDef.INSTANCE);

        if (currentTrait.equals(targetTrait))
            return rel;

        if (currentTrait.equals(HazelcastDistributionTrait.ANY) && !(rel instanceof RelSubset) )
            return null;

        if (rel.getConvention() != HazelcastPhysicalRel.HAZELCAST_PHYSICAL)
            return null;

        switch (targetTrait.getType()){
            case SINGLETON:
                return new HazelcastSingletonExchangePhysicalRel(
                    rel.getCluster(),
                    planner.emptyTraitSet().plus(HazelcastPhysicalRel.HAZELCAST_PHYSICAL).plus(targetTrait),
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
        HazelcastDistributionTrait fromTrait,
        HazelcastDistributionTrait toTrait
    ) {
        return true;
    }

    @Override
    public HazelcastDistributionTrait getDefault() {
        return HazelcastDistributionTrait.ANY;
    }
}
