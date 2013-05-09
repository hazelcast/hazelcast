package com.hazelcast.spi.impl;

import com.hazelcast.nio.serialization.*;
import com.hazelcast.security.UsernamePasswordCredentials;

import java.util.Collection;

/**
 * @mdogan 4/30/13
 */
public final class SpiPortableHook implements PortableHook {

    public static int ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.SPI_PORTABLE_FACTORY, -1);

    public static final int USERNAME_PWD_CRED = 1;

    public int getFactoryId() {
        return ID;
    }

    public PortableFactory createFactory() {
        return new PortableFactory() {
            public Portable create(int classId) {
                return classId == USERNAME_PWD_CRED ? new UsernamePasswordCredentials() : null;
            }
        };
    }

    public Collection<ClassDefinition> getBuiltinDefinitions() {
        return null;
    }
}
