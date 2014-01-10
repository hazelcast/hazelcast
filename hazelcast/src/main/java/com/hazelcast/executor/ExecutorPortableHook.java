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

package com.hazelcast.executor;

import com.hazelcast.executor.client.IsShutdownRequest;
import com.hazelcast.executor.client.LocalTargetCallableRequest;
import com.hazelcast.executor.client.TargetCallableRequest;
import com.hazelcast.nio.serialization.*;

import java.util.Collection;

/**
 * @author mdogan 5/13/13
 */
public final class ExecutorPortableHook implements PortableHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.EXECUTOR_PORTABLE_FACTORY, -13);

    public static final int IS_SHUTDOWN_REQUEST = 1;
    public static final int LOCAL_TARGET_CALLABLE_REQUEST = 2;
    public static final int TARGET_CALLABLE_REQUEST = 3;

    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public PortableFactory createFactory() {
        return new PortableFactory() {
            public Portable create(int classId) {
                switch (classId) {
                    case IS_SHUTDOWN_REQUEST:
                        return new IsShutdownRequest();
                    case LOCAL_TARGET_CALLABLE_REQUEST:
                        return new LocalTargetCallableRequest();
                    case TARGET_CALLABLE_REQUEST:
                        return new TargetCallableRequest();
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
