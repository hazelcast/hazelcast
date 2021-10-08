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
import com.hazelcast.jet.sql.impl.opt.logical.SelectByKeyMapLogicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;
import static com.hazelcast.jet.sql.impl.opt.Conventions.PHYSICAL;

final class SelectByKeyMapPhysicalRule extends ConverterRule {

    static final RelOptRule INSTANCE = new SelectByKeyMapPhysicalRule();

    private SelectByKeyMapPhysicalRule() {
        super(
                SelectByKeyMapLogicalRel.class, LOGICAL, PHYSICAL,
                SelectByKeyMapPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public RelNode convert(RelNode rel) {
        SelectByKeyMapLogicalRel logicalSelect = (SelectByKeyMapLogicalRel) rel;

        return new SelectByKeyMapPhysicalRel(
                logicalSelect.getCluster(),
                OptUtils.toPhysicalConvention(logicalSelect.getTraitSet()),
                logicalSelect.getRowType(),
                logicalSelect.table(),
                logicalSelect.keyCondition(),
                logicalSelect.projections()
        );
    }
}
