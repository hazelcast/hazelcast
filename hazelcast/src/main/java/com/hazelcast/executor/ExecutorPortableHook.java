package com.hazelcast.executor;

import com.hazelcast.nio.serialization.*;

import java.util.Collection;

/**
 * @mdogan 5/13/13
 */
public final class ExecutorPortableHook implements PortableHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.EXECUTOR_PORTABLE_FACTORY, -13);

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
