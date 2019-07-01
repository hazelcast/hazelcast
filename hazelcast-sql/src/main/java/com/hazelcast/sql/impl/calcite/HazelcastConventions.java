package com.hazelcast.sql.impl.calcite;

import com.hazelcast.sql.impl.calcite.physical.rel.PhysicalRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;

/**
 * Hazelcast conventions.
 */
public class HazelcastConventions {
    public static Convention HAZELCAST_PHYSICAL = new Convention.Impl("PHYSICAL", PhysicalRel.class) {
        @Override
        public boolean canConvertConvention(Convention toConvention) {
            return true;
        }

        @Override
        public boolean useAbstractConvertersForConversion(RelTraitSet fromTraits, RelTraitSet toTraits) {
            return true;
        }
    };

    private HazelcastConventions() {
        // No-op.
    }
}
