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
