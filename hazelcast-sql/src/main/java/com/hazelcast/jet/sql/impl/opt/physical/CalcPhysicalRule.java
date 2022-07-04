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

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.CalcLogicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;
import static com.hazelcast.jet.sql.impl.opt.Conventions.PHYSICAL;

final class CalcPhysicalRule extends ConverterRule {

    /**
     * Default configuration.
     */
    private static final Config DEFAULT_CONFIG = Config.INSTANCE
            .withConversion(CalcLogicalRel.class, LOGICAL, PHYSICAL, CalcPhysicalRule.class.getSimpleName());

    @SuppressWarnings("checkstyle:DeclarationOrder")
    static final RelOptRule INSTANCE = new CalcPhysicalRule();

    private CalcPhysicalRule() {
        super(DEFAULT_CONFIG);
    }

    @Override
    public RelNode convert(RelNode rel) {
        CalcLogicalRel calc = (CalcLogicalRel) rel;

        RelNode transformedInput = RelOptRule.convert(calc.getInput(), calc.getInput().getTraitSet().replace(PHYSICAL));

        return new CalcPhysicalRel(
                calc.getCluster(),
                OptUtils.toPhysicalConvention(transformedInput.getTraitSet()),
                OptUtils.toPhysicalInput(transformedInput),
                calc.getProgram()
        );
    }
}
