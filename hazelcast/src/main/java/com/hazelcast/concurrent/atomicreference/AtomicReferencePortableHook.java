package com.hazelcast.concurrent.atomicreference;

import com.hazelcast.concurrent.atomicreference.client.*;
import com.hazelcast.nio.serialization.*;

import java.util.Collection;

public class AtomicReferencePortableHook implements PortableHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.ATOMIC_REFERENCE_PORTABLE_FACTORY, -21);

    public static final int GET = 1;
    public static final int SET = 2;
    public static final int GET_AND_SET = 3;
    public static final int IS_NULL = 4;
    public static final int COMPARE_AND_SET = 5;
    public static final int CONTAINS = 6;
    public static final int APPLY = 7;
    public static final int ALTER = 8;
    public static final int ALTER_AND_GET = 9;
    public static final int GET_AND_ALTER = 10;

    public int getFactoryId() {
        return F_ID;
    }

    public PortableFactory createFactory() {
        return new PortableFactory() {
            public Portable create(int classId) {
                switch (classId) {
                    case GET:
                        return new GetRequest();
                    case SET:
                        return new SetRequest();
                    case GET_AND_SET:
                        return new GetAndSetRequest();
                    case IS_NULL:
                        return new IsNullRequest();
                    case COMPARE_AND_SET:
                        return new CompareAndSetRequest();
                    case CONTAINS:
                        return new ContainsRequest();
                    case APPLY:
                        return new ApplyRequest();
                    case ALTER:
                        return new AlterRequest();
                    case ALTER_AND_GET:
                        return new AlterAndGetRequest();
                    case GET_AND_ALTER:
                        return new GetAndAlterRequest();
                }
                return null;
            }
        };
    }

    @Override
    public Collection<ClassDefinition> getBuiltinDefinitions() {
        return null;
    }
}
