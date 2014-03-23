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

package com.hazelcast.concurrent.countdownlatch.client;

import com.hazelcast.nio.serialization.*;
import java.util.Collection;

public final class CountDownLatchPortableHook implements PortableHook {

    static final int F_ID = FactoryIdRepository.getPortableFactoryId(FactoryIdRepository.CDL);

    static final int COUNT_DOWN = 1;
    static final int AWAIT = 2;
    static final int SET_COUNT = 3;
    static final int GET_COUNT = 4;

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
                    case COUNT_DOWN:
                        return new CountDownRequest();
                    case AWAIT:
                        return new AwaitRequest();
                    case SET_COUNT:
                        return new SetCountRequest();
                    case GET_COUNT:
                        return new GetCountRequest();
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
