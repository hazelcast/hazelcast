package com.hazelcast.web;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class WebDataSerializableFactory implements DataSerializableFactory {

    @Override
    public IdentifiedDataSerializable create(int typeId) {
        if (typeId == WebDataSerializerHook.SESSION_ATTRIBUTE_ID){
            return new SessionAttributePredicate();
        }
        throw new IllegalArgumentException();
    }
}
