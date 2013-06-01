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

package com.hazelcast.concurrent.semaphore;

import com.hazelcast.concurrent.semaphore.client.*;
import com.hazelcast.nio.serialization.*;

import java.util.Collection;

/**
 * @ali 5/13/13
 */
public class SemaphorePortableHook implements PortableHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.SEMAPHORE_PORTABLE_FACTORY, -16);

    public static final int ACQUIRE = 1;

    public static final int AVAILABLE = 2;

    public static final int DRAIN = 3;

    public static final int INIT = 4;

    public static final int REDUCE = 5;

    public static final int RELEASE = 6;

    public static final int DESTROY = 7;

    public int getFactoryId() {
        return F_ID;
    }

    public PortableFactory createFactory() {
        return new PortableFactory(){

            public Portable create(int classId) {
                switch (classId){
                    case ACQUIRE:
                        return new AcquireRequest();
                    case AVAILABLE:
                        return new AvailableRequest();
                    case DRAIN:
                        return new DrainRequest();
                    case INIT:
                        return new InitRequest();
                    case REDUCE:
                        return new ReduceRequest();
                    case RELEASE:
                        return new ReleaseRequest();
                    case DESTROY:
                        return new SemaphoreDestroyRequest();
                }
                return null;
            }
        };
    }

    public Collection<ClassDefinition> getBuiltinDefinitions() {
        return null;
    }
}
