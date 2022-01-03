package com.hazelcast.jet.sql.impl.opt;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;

import javax.annotation.Nullable;

import static java.util.Collections.emptyList;

public abstract class FullScan extends TableScan {

    private final FunctionEx<ExpressionEvalContext, EventTimePolicy<Object[]>> eventTimePolicyProvider;
    private final int watermarkedColumnIndex;

    protected FullScan(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            @Nullable FunctionEx<ExpressionEvalContext, EventTimePolicy<Object[]>> eventTimePolicyProvider,
            int watermarkedColumnIndex
    ) {
        super(cluster, traitSet, emptyList(), table);
        assert watermarkedColumnIndex < 0 ^ eventTimePolicyProvider != null;

        this.eventTimePolicyProvider = eventTimePolicyProvider;
        this.watermarkedColumnIndex = watermarkedColumnIndex;
    }

    @Nullable
    public FunctionEx<ExpressionEvalContext, EventTimePolicy<Object[]>> eventTimePolicyProvider() {
        return eventTimePolicyProvider;
    }

    public int watermarkedColumnIndex() {
        return watermarkedColumnIndex;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .itemIf("eventTimePolicyProvider", eventTimePolicyProvider, eventTimePolicyProvider != null)
                .item("watermarkedColumnIndex", watermarkedColumnIndex);
    }
}
