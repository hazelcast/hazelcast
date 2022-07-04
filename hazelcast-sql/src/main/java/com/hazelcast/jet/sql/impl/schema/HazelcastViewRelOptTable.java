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

package com.hazelcast.jet.sql.impl.schema;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;

public class HazelcastViewRelOptTable extends HazelcastRelOptTable {
    private final RelNode viewRel;

    public HazelcastViewRelOptTable(Prepare.PreparingTable delegate, RelNode viewRel) {
        super(delegate);
        this.viewRel = viewRel;
    }

    @Override
    public final RelNode toRel(RelOptTable.ToRelContext context) {
        return RelOptUtil.createCastRel(viewRel, getRowType(), true);
    }
}
