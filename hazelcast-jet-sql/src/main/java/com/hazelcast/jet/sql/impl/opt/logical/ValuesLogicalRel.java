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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ValuesLogicalRel extends AbstractRelNode implements LogicalRel {

    private final RelDataType rowType;
    private final List<Object[]> tuples;

    ValuesLogicalRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelDataType rowType,
            List<Object[]> tuples
    ) {
        super(cluster, traits);

        this.rowType = rowType;
        this.tuples = tuples;
    }

    public List<Object[]> tuples() {
        return tuples;
    }

    @Override
    protected RelDataType deriveRowType() {
        return rowType;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new ValuesLogicalRel(getCluster(), traitSet, rowType, tuples);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("tuples",
                        tuples.stream()
                                .map(row -> Arrays.stream(row)
                                        .map(String::valueOf)
                                        .collect(Collectors.joining(", ", "{ ", " }")))
                                .collect(Collectors.joining(", ", "[", "]"))
                );
    }
}
