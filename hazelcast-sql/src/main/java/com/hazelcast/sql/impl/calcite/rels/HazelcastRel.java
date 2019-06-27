package com.hazelcast.sql.impl.calcite.rels;

import com.hazelcast.sql.impl.calcite.SqlCacitePlanVisitor;
import org.apache.calcite.plan.Convention;

public interface HazelcastRel extends HazelcastRelNode {
    Convention LOGICAL = new Convention.Impl("LOGICAL", HazelcastRel.class);

    void visitForPlan(SqlCacitePlanVisitor visitor);
}
