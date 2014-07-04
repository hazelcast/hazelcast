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

package com.hazelcast.concurrent.lock.client;

import com.hazelcast.nio.serialization.AbstractPortableHook;
import com.hazelcast.nio.serialization.ArrayPortableFactory;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.util.ConstructorFunction;

public class LockPortableHook extends AbstractPortableHook {

    public static final int FACTORY_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.LOCK_PORTABLE_FACTORY, -15);

    public static final int LOCK = 1;
    public static final int UNLOCK = 2;
    public static final int IS_LOCKED = 3;
    public static final int GET_LOCK_COUNT = 5;
    public static final int GET_REMAINING_LEASE = 6;
    public static final int CONDITION_BEFORE_AWAIT = 7;
    public static final int CONDITION_AWAIT = 8;
    public static final int CONDITION_SIGNAL = 9;

    @Override
    public int getFactoryId() {
        return FACTORY_ID;
    }

    @Override
    public PortableFactory createFactory() {
        ConstructorFunction<Integer, Portable>[] constructors = new ConstructorFunction[CONDITION_SIGNAL + 1];
        constructors[LOCK] = createFunction(new LockRequest());
        constructors[UNLOCK] = createFunction(new UnlockRequest());
        constructors[IS_LOCKED] = createFunction(new IsLockedRequest());
        constructors[GET_LOCK_COUNT] = createFunction(new GetLockCountRequest());
        constructors[GET_REMAINING_LEASE] = createFunction(new GetRemainingLeaseRequest());
        constructors[CONDITION_BEFORE_AWAIT] = createFunction(new BeforeAwaitRequest());
        constructors[CONDITION_AWAIT] = createFunction(new AwaitRequest());
        constructors[CONDITION_SIGNAL] = createFunction(new SignalRequest());
        return new ArrayPortableFactory(constructors);
    }
}
