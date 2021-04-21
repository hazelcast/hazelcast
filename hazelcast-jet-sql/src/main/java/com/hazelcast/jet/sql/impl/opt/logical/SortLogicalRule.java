/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.opt.logical;

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Sort;

import static com.hazelcast.jet.sql.impl.opt.JetConventions.LOGICAL;

public final class SortLogicalRule extends ConverterRule {

    static final RelOptRule INSTANCE = new SortLogicalRule();

    private SortLogicalRule() {
        super(
                Sort.class, Convention.NONE, LOGICAL,
                SortLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public RelNode convert(RelNode rel) {
        Sort sort = (Sort) rel;

        return new SortLogicalRel(
                sort.getCluster(),
                OptUtils.toLogicalConvention(sort.getTraitSet()),
                OptUtils.toLogicalInput(sort.getInput()),
                sort.getCollation(),
                sort.offset,
                sort.fetch
        );
    }
}
