package com.hazelcast.map;

import com.hazelcast.map.clientv2.MapGetRequest;
import com.hazelcast.map.clientv2.MapPutRequest;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableHook;

/**
 * @mdogan 5/2/13
 */
public class MapPortableHook implements PortableHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.MAP_PORTABLE_FACTORY, -10);

    public static final int GET = 1;
    public static final int PUT = 2;

    public int getFactoryId() {
        return F_ID;
    }

    public PortableFactory createFactory() {
        return new PortableFactory() {
            public Portable create(int classId) {
                switch (classId) {
                    case 1:
                        return new MapGetRequest();
                    case 2:
                        return new MapPutRequest();
                }
                return null;
            }
        };
    }
}
