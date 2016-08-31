package com.hazelcast.cardinality.hyperloglog;

import com.hazelcast.cardinality.hyperloglog.operations.AddHashOperation;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.HLL_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.HLL_DS_FACTORY_ID;

public class HyperLogLogDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(HLL_DS_FACTORY, HLL_DS_FACTORY_ID);

    public static final int ADD_HASH = 0;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {
            @Override
            public IdentifiedDataSerializable create(int typeId) {
                switch (typeId) {
                    case ADD_HASH:
                        return new AddHashOperation();
                    default:
                        return null;
                }
            }
        };
    }
}
