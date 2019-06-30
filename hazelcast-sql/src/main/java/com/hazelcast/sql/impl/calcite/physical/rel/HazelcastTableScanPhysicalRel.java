/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.physical.rel;

import com.hazelcast.sql.impl.calcite.SqlCalcitePlanVisitor;
import com.hazelcast.sql.impl.calcite.logical.rel.HazelcastRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

// TODO: getDigest - should we implement it?
public class HazelcastTableScanPhysicalRel extends TableScan implements HazelcastPhysicalRel {

    private final RelDataType rowType;

    public HazelcastTableScanPhysicalRel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
        this(cluster, traitSet, table, table.getRowType());
    }

    public HazelcastTableScanPhysicalRel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, RelDataType rowType) {
        super(cluster, traitSet, table);

        this.rowType = rowType;
    }

    @Override
    public RelDataType deriveRowType() {
        return rowType;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new HazelcastTableScanPhysicalRel(this.getCluster(), traitSet, this.getTable(), this.rowType);
    }
}
