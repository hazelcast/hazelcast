package com.hazelcast.web;

import com.hazelcast.nio.serialization.*;

public class WebDataSerializerHook implements DataSerializerHook{

    public static final int F_ID = FactoryIdRepository.getDSFactoryId(FactoryIdRepository.WEB);

    public static final int SESSION_ATTRIBUTE_ID = 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {
            @Override
            public IdentifiedDataSerializable create(int typeId) {
                if (typeId == SESSION_ATTRIBUTE_ID){
                    return new SessionAttributePredicate();
                }
                throw new IllegalArgumentException();
            }
        };
    }
}
