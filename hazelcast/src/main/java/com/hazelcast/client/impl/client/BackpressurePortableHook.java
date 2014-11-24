package com.hazelcast.client.impl.client;

import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableHook;

import java.util.Collection;

public class BackpressurePortableHook implements PortableHook {
    static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.BACKPRESSURE_PORTABLE_FACTORY, -25);

    static final int SLOT_REQUEST = 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public PortableFactory createFactory() {
        return new PortableFactory() {
            @Override
            public Portable create(int classId) {
                switch (classId) {
                    case SLOT_REQUEST :
                        return new GetSlotsRequest();
                    default:
                        return null;
                }
            };
        };
    }

    @Override
    public Collection<ClassDefinition> getBuiltinDefinitions() {
        return null;
    }
}
