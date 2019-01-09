/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.ringbuffer.impl.client;

import com.hazelcast.internal.serialization.PortableHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;

import java.util.Collection;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.RINGBUFFER_PORTABLE_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.RINGBUFFER_PORTABLE_FACTORY_ID;

/**
 * Provides a Portable hook for the ringbuffer operations.
 */
public class RingbufferPortableHook implements PortableHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(RINGBUFFER_PORTABLE_FACTORY, RINGBUFFER_PORTABLE_FACTORY_ID);

    public static final int READ_RESULT_SET = 10;

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
                    case READ_RESULT_SET:
                        return new PortableReadResultSet<Object>();
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
