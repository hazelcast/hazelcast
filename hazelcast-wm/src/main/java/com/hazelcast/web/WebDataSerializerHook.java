package com.hazelcast.web;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class WebDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.WEB_DS_FACTORY, F_ID_OFFSET_WEBMODULE);

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
                if (typeId == SESSION_ATTRIBUTE_ID) {
                    return new SessionAttributePredicate();
                }
                throw new IllegalArgumentException();
            }
        };
    }
}
