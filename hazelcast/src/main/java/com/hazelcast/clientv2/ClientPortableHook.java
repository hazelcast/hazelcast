package com.hazelcast.clientv2;

import com.hazelcast.nio.serialization.*;

import java.util.Collection;
import java.util.Collections;

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

    public Collection<ClassDefinition> getBuiltinDefinitions() {
        ClassDefinitionBuilder builder = new ClassDefinitionBuilder(ID, PRINCIPAL);
        builder.addUTFField("uuid").addUTFField("ownerUuid");
        return Collections.singleton(builder.build());
    }
}
