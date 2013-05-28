package com.hazelcast.concurrent.countdownlatch;

import com.hazelcast.concurrent.countdownlatch.client.*;
import com.hazelcast.nio.serialization.*;

import java.util.Collection;

/**
 * @mdogan 5/14/13
 */
public final class CountDownLatchPortableHook implements PortableHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.CDL_PORTABLE_FACTORY, -14);

    public static final int COUNT_DOWN = 1;
    public static final int AWAIT = 2;
    public static final int SET_COUNT = 3;
    public static final int GET_COUNT = 4;
    public static final int DESTROY = 5;


    public int getFactoryId() {
        return F_ID;
    }

    public PortableFactory createFactory() {
        return new PortableFactory() {
            public Portable create(int classId) {
                switch (classId) {
                    case COUNT_DOWN:
                        return new CountDownRequest();
                    case AWAIT:
                        return new AwaitRequest();
                    case SET_COUNT:
                        return new SetCountRequest();
                    case GET_COUNT:
                        return new GetCountRequest();
                    case DESTROY:
                        return new CountDownLatchDestroyRequest();
                }
                return null;
            }
        };
    }

    public Collection<ClassDefinition> getBuiltinDefinitions() {
        return null;
    }
}
