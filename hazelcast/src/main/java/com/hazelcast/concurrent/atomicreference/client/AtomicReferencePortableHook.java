/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.concurrent.atomicreference.client;

import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableHook;

import java.util.Collection;

public class AtomicReferencePortableHook implements PortableHook {

    static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.ATOMIC_REFERENCE_PORTABLE_FACTORY, -21);

    static final int GET = 1;
    static final int SET = 2;
    static final int GET_AND_SET = 3;
    static final int IS_NULL = 4;
    static final int COMPARE_AND_SET = 5;
    static final int CONTAINS = 6;
    static final int APPLY = 7;
    static final int ALTER = 8;
    static final int ALTER_AND_GET = 9;
    static final int GET_AND_ALTER = 10;

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
                    default:
                        return null;
                }
            }
        };
    }

    @Override
    public Collection<ClassDefinition> getBuiltinDefinitions() {
        return null;
    }
}
