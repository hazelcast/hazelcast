package com.hazelcast.partition;

import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableHook;

import java.util.Collection;

/**
 * @mdogan 5/13/13
 */
public final class PartitionPortableHook implements PortableHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.PARTITION_PORTABLE_FACTORY, -2);

    public int getFactoryId() {
        return F_ID;
    }

    public PortableFactory createFactory() {
        return null;
    }

    public Collection<ClassDefinition> getBuiltinDefinitions() {
        return null;
    }
}
