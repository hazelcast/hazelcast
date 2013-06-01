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

package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.concurrent.atomiclong.client.*;
import com.hazelcast.nio.serialization.*;

import java.util.Collection;

/**
 * @ali 5/13/13
 */
public class AtomicLongPortableHook implements PortableHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.ATOMIC_LONG_PORTABLE_FACTORY, -17);

    public static final int ADD_AND_GET = 1;
    public static final int COMPARE_AND_SET = 2;
    public static final int GET_AND_ADD = 3;
    public static final int GET_AND_SET = 4;
    public static final int SET = 5;
    public static final int DESTROY = 6;


    public int getFactoryId() {
        return F_ID;
    }

    public PortableFactory createFactory() {
        return new PortableFactory() {
            public Portable create(int classId) {
                switch (classId){
                    case ADD_AND_GET:
                        return new AddAndGetRequest();
                    case COMPARE_AND_SET:
                        return new CompareAndSetRequest();
                    case GET_AND_ADD:
                        return new GetAndAddRequest();
                    case GET_AND_SET:
                        return new GetAndSetRequest();
                    case SET:
                        return new SetRequest();
                    case DESTROY:
                        return new AtomicLongDestroyRequest();
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
