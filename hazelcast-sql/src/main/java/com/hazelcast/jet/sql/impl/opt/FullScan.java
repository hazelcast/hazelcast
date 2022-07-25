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

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;

import javax.annotation.Nullable;

import static java.util.Collections.emptyList;

public abstract class FullScan extends TableScan {

    protected final BiFunctionEx<ExpressionEvalContext, Byte, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider;
    protected final int watermarkedColumnIndex;

    protected FullScan(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            @Nullable BiFunctionEx<ExpressionEvalContext, Byte, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider,
            int watermarkedColumnIndex
    ) {
        super(cluster, traitSet, emptyList(), table);
        this.eventTimePolicyProvider = eventTimePolicyProvider;
        this.watermarkedColumnIndex = watermarkedColumnIndex;
    }

    @Nullable
    public BiFunctionEx<ExpressionEvalContext, Byte, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider() {
        return eventTimePolicyProvider;
    }

    public int watermarkedColumnIndex() {
        return watermarkedColumnIndex;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        String eventPolicyPresence = eventTimePolicyProvider != null ? "Present" : "Absent";
        return super.explainTerms(pw)
                .itemIf("eventTimePolicyProvider", eventPolicyPresence, eventTimePolicyProvider != null)
                .itemIf("watermarkedColumnIndex", watermarkedColumnIndex, watermarkedColumnIndex >= 0);
    }
}
