package com.hazelcast.clientv2;

import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableHook;

/**
 * @mdogan 4/30/13
 */
public class ClientPortableHook implements PortableHook {

    public static final int ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.CLIENT_PORTABLE_FACTORY, -3);

    public static final int PRINCIPAL = 3;

    public int getFactoryId() {
        return ID;
    }

    public PortableFactory createFactory() {
        return new ClientPortableFactory();
    }
}
