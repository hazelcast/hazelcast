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

    protected FullScan(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            @Nullable FunctionEx<ExpressionEvalContext, EventTimePolicy<Object[]>> eventTimePolicyProvider
    ) {
        super(cluster, traitSet, emptyList(), table);

        this.eventTimePolicyProvider = eventTimePolicyProvider;
    }

    @Nullable
    public FunctionEx<ExpressionEvalContext, EventTimePolicy<Object[]>> eventTimePolicyProvider() {
        return eventTimePolicyProvider;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .itemIf("eventTimePolicyProvider", eventTimePolicyProvider, eventTimePolicyProvider != null);
    }
}
