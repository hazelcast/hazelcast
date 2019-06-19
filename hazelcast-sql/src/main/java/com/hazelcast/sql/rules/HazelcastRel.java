package com.hazelcast.sql.rules;

import com.jayway.jsonpath.internal.filter.LogicalOperator;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

public interface HazelcastRel extends HazelcastRelNode {
    Convention LOGICAL = new Convention.Impl("LOGICAL", HazelcastRel.class);

    // TODO: Visitor to produce implementation.
}
