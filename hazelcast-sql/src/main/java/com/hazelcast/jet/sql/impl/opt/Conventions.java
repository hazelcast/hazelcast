/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.opt;

import com.hazelcast.jet.sql.impl.opt.logical.LogicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;

public final class Conventions {

    public static final Convention LOGICAL = new Convention.Impl("LOGICAL", LogicalRel.class);

    public static final Convention PHYSICAL = new Convention.Impl("PHYSICAL", PhysicalRel.class) {
        @Override
        public boolean canConvertConvention(Convention toConvention) {
            return true;
        }

        @Override
        public boolean useAbstractConvertersForConversion(RelTraitSet fromTraits, RelTraitSet toTraits) {
            return !fromTraits.satisfies(toTraits);
        }
    };

    private Conventions() {
    }
}
