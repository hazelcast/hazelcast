package com.hazelcast.monitor;

import com.hazelcast.management.JsonSerializable;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicate;

public interface IndexStats extends DataSerializable, JsonSerializable {
    long getIndexEntryCount();
    long getUsageCount();
    Predicate getPredicate();
    String getAttributeName();
}
