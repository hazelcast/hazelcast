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

package com.hazelcast.concurrent.semaphore.client;

import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableHook;

import java.util.Collection;

public class SemaphorePortableHook implements PortableHook {

    static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.SEMAPHORE_PORTABLE_FACTORY, -16);
    static final int ACQUIRE = 1;
    static final int AVAILABLE = 2;
    static final int DRAIN = 3;
    static final int INIT = 4;
    static final int REDUCE = 5;
    static final int RELEASE = 6;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public PortableFactory createFactory() {
        return new PortableFactory() {
            @Override
            public Portable create(int classId) {
                switch (classId) {
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
