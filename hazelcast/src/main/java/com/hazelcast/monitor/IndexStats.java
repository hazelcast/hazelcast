package com.hazelcast.monitor;

import com.hazelcast.management.JsonSerializable;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicate;

/**
 * Created by buremba <Burak Emre Kabakcı> on 22/08/14 16:11.
 */
public interface IndexStats extends DataSerializable, JsonSerializable {
    long getIndexItemCount();
    long getUsageCount();
    Predicate getPredicate();
    String getAttributeName();
}
