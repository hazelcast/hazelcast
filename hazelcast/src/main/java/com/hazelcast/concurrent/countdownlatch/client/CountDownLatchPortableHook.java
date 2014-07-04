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

import com.hazelcast.nio.serialization.AbstractPortableHook;
import com.hazelcast.nio.serialization.ArrayPortableFactory;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.util.ConstructorFunction;

public final class CountDownLatchPortableHook extends AbstractPortableHook {

    static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.CDL_PORTABLE_FACTORY, -14);

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
        ConstructorFunction<Integer, Portable>[] constructors = new ConstructorFunction[GET_COUNT + 1];
        constructors[COUNT_DOWN] = createFunction(new CountDownRequest());
        constructors[AWAIT] = createFunction(new AwaitRequest());
        constructors[SET_COUNT] = createFunction(new SetCountRequest());
        constructors[GET_COUNT] = createFunction(new GetCountRequest());
        return new ArrayPortableFactory(constructors);
    }
}
