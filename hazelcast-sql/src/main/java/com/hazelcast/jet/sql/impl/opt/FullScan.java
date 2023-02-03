/*
 * Copyright 2023 Hazelcast Inc.
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

import com.hazelcast.sql.impl.expression.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;

import javax.annotation.Nullable;

import static java.util.Collections.emptyList;

public abstract class FullScan extends TableScan {

    protected final Expression<?> lagExpression;
    protected final int watermarkedColumnIndex;

    protected FullScan(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            @Nullable Expression<?> lagExpression,
            int watermarkedColumnIndex
    ) {
        super(cluster, traitSet, emptyList(), table);
        assert watermarkedColumnIndex < 0 ^ lagExpression != null;
        this.lagExpression = lagExpression;
        this.watermarkedColumnIndex = watermarkedColumnIndex;
    }

    @Nullable
    public Expression<?> lagExpression() {
        return lagExpression;
    }

    public int watermarkedColumnIndex() {
        return watermarkedColumnIndex;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .itemIf("lagExpression", "Present", lagExpression != null)
                .itemIf("watermarkedColumnIndex", watermarkedColumnIndex, watermarkedColumnIndex >= 0);
    }
}
