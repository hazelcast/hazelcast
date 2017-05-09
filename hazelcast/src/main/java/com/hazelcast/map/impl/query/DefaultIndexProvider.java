package com.hazelcast.map.impl.query;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.IndexImpl;
import com.hazelcast.query.impl.getters.Extractors;

public class DefaultIndexProvider implements IndexProvider {
    @Override
    public Index createIndex(String attributeName, boolean ordered, Extractors extractors, InternalSerializationService ss) {
        return new IndexImpl(attributeName, ordered, ss, extractors);
    }
}
