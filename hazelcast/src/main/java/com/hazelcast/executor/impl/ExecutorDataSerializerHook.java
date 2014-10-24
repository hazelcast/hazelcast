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

package com.hazelcast.executor.impl;

import com.hazelcast.executor.impl.operations.CallableTaskOperation;
import com.hazelcast.executor.impl.operations.MemberCallableTaskOperation;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class ExecutorDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.EXECUTOR_DS_FACTORY, -13);

    public static final int CALLABLE_TASK = 0;
    public static final int MEMBER_CALLABLE_TASK = 1;
    static final int RUNNABLE_ADAPTER = 2;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {
            @Override
            public IdentifiedDataSerializable create(int typeId) {
                switch (typeId) {
                    case CALLABLE_TASK:
                        return new CallableTaskOperation();
                    case MEMBER_CALLABLE_TASK:
                        return new MemberCallableTaskOperation();
                    case RUNNABLE_ADAPTER:
                        return new RunnableAdapter();
                    default:
                        return null;
                }
            }
        };
    }
}
