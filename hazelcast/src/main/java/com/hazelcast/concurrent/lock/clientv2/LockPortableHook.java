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

package com.hazelcast.concurrent.lock.clientv2;

import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableHook;

/**
 * @mdogan 5/3/13
 */
public class LockPortableHook implements PortableHook {

    static final int FACTORY_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.LOCK_PORTABLE_FACTORY, -15);

    @Override
    public int getFactoryId() {
        return FACTORY_ID;
    }

    @Override
    public PortableFactory createFactory() {
        return new PortableFactory() {
            public Portable create(int classId) {
                if (classId == 1) {
                    return new LockRequest();
                } else {
                    return new UnlockRequest();
                }
            }
        };
    }
}
