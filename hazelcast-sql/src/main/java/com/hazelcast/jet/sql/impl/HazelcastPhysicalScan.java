package com.hazelcast.jet.sql.impl;

import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRel;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.expression.Expression;

public interface HazelcastPhysicalScan extends PhysicalRel {

    Expression<Boolean> filter(QueryParameterMetadata parameterMetadata);
}
