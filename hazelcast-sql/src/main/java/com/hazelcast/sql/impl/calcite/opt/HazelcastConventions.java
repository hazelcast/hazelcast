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

package com.hazelcast.sql.impl.calcite.opt;

import com.hazelcast.sql.impl.calcite.opt.logical.LogicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.PhysicalRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;

/**
 * Hazelcast conventions.
 */
public final class HazelcastConventions {
    /** Convention used during logical planning. */
    public static final Convention LOGICAL = new Convention.Impl("LOGICAL", LogicalRel.class);

    /** Convention used during physical planning. */
    public static final Convention PHYSICAL = new Convention.Impl("PHYSICAL", PhysicalRel.class) {
        @Override
        public boolean canConvertConvention(Convention toConvention) {
            // Allows conversion between LOGICAL and PHYSICAL conventions.
            assert toConvention == LOGICAL;

            return true;
        }

        @Override
        public boolean useAbstractConvertersForConversion(RelTraitSet fromTraits, RelTraitSet toTraits) {
            // Forces Apache Calcite to install abstract converters when traits of two relations do not satisfy each other.
            // The converter will have LOGICAL "from" convention and PHYSICAL "to" convention. As a result, installation
            // of such a converter into a search space will re-trigger rules on LOGICAL parents, giving these rules a chance
            // to propagate physical traits from children nodes.
            return !fromTraits.satisfies(toTraits);
        }
    };

    private HazelcastConventions() {
        // No-op.
    }
}
