package com.hazelcast.map.impl.query;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.getters.Extractors;

public interface IndexProvider {

    Index createIndex(String attributeName, boolean ordered, Extractors extractors, InternalSerializationService ss);

}
