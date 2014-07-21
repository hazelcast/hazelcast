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

import com.hazelcast.executor.client.CancellationRequest;
import com.hazelcast.executor.client.IsShutdownRequest;
import com.hazelcast.executor.client.PartitionCallableRequest;
import com.hazelcast.executor.client.ShutdownRequest;
import com.hazelcast.executor.client.TargetCallableRequest;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableHook;

import java.util.Collection;

public final class ExecutorPortableHook implements PortableHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.EXECUTOR_PORTABLE_FACTORY, -13);

    public static final int IS_SHUTDOWN_REQUEST = 1;
    public static final int CANCELLATION_REQUEST = 2;
    public static final int TARGET_CALLABLE_REQUEST = 3;
    public static final int PARTITION_CALLABLE_REQUEST = 4;
    public static final int SHUTDOWN_REQUEST = 5;

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
                    case IS_SHUTDOWN_REQUEST:
                        return new IsShutdownRequest();
                    case CANCELLATION_REQUEST:
                        return new CancellationRequest();
                    case TARGET_CALLABLE_REQUEST:
                        return new TargetCallableRequest();
                    case PARTITION_CALLABLE_REQUEST:
                        return new PartitionCallableRequest();
                    case SHUTDOWN_REQUEST:
                        return new ShutdownRequest();
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
