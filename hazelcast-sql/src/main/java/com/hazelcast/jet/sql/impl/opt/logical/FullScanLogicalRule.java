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

package com.hazelcast.jet.sql.impl.opt.logical;

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;

final class FullScanLogicalRule extends ConverterRule {

    static final RelOptRule INSTANCE = new FullScanLogicalRule();

    private FullScanLogicalRule() {
        super(
                LogicalTableScan.class, Convention.NONE, LOGICAL,
                FullScanLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public RelNode convert(RelNode rel) {
        TableScan scan = (LogicalTableScan) rel;

        return new FullScanLogicalRel(
                scan.getCluster(),
                OptUtils.toLogicalConvention(scan.getTraitSet()),
                scan.getTable(),
                null,
                -1);
    }
}
