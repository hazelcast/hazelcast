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

package com.hazelcast.sql.impl.calcite.opt.physical;

import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Expand conversion rule that is a copy of Calcite's one. The only difference is that
 * in the changeTraitsUsingConverters method it uses satisfies method for traits instead
 * of equals so that, for instance, index scan with collation satisfieы empty collation trait.
 * We consider that like a bug in Calcite.
 */
public class ExpandConversionRule extends RelOptRule {
    public static final ExpandConversionRule INSTANCE =
            new ExpandConversionRule(RelFactories.LOGICAL_BUILDER);

    /**
     * Creates an ExpandConversionRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public ExpandConversionRule(RelBuilderFactory relBuilderFactory) {
        super(operand(AbstractConverter.class, any()), relBuilderFactory, null);
    }

    public void onMatch(RelOptRuleCall call) {
        final VolcanoPlanner planner = (VolcanoPlanner) call.getPlanner();
        AbstractConverter converter = call.rel(0);
        final RelNode child = converter.getInput();
        RelNode converted =
                changeTraitsUsingConverters(
                        planner,
                        child,
                        converter.getTraitSet());
        if (converted != null) {
            call.transformTo(converted);
        }
    }

    private RelNode changeTraitsUsingConverters(VolcanoPlanner planner,
                                                RelNode rel,
                                                RelTraitSet toTraits) {
        final RelTraitSet fromTraits = rel.getTraitSet();

        assert fromTraits.size() >= toTraits.size();

        final boolean allowInfiniteCostConverters =
                CalciteSystemProperty.ALLOW_INFINITE_COST_CONVERTERS.value();

        // Traits may build on top of another...for example a collation trait
        // would typically come after a distribution trait since distribution
        // destroys collation; so when doing the conversion below we use
        // fromTraits as the trait of the just previously converted RelNode.
        // Also, toTraits may have fewer traits than fromTraits, excess traits
        // will be left as is.  Finally, any null entries in toTraits are
        // ignored.
        RelNode converted = rel;
        for (int i = 0; (converted != null) && (i < toTraits.size()); i++) {
            RelTrait fromTrait = converted.getTraitSet().getTrait(i);
            final RelTraitDef traitDef = fromTrait.getTraitDef();
            RelTrait toTrait = toTraits.getTrait(i);

            if (toTrait == null) {
                continue;
            }

            assert traitDef == toTrait.getTraitDef();

            // Calcite FIX: We change the original fromTrait.equals(toTrait)
            if (fromTrait.satisfies(toTrait)) {
                // No need to convert; it's already correct.
                continue;
            }

            rel = traitDef.convert(
                    planner,
                    converted,
                    toTrait,
                    allowInfiniteCostConverters);
            if (rel != null) {
                assert rel.getTraitSet().getTrait(traitDef).satisfies(toTrait);
                planner.register(rel, converted);
            }

            converted = rel;
        }

        // make sure final converted traitset subsumes what was required
        if (converted != null) {
            assert converted.getTraitSet().satisfies(toTraits);
        }

        return converted;
    }
}
