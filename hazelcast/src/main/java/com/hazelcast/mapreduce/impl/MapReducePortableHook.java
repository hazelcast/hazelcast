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

package com.hazelcast.mapreduce.impl;

import com.hazelcast.internal.serialization.PortableHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.mapreduce.impl.task.TransferableJobProcessInformation;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collection;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.MAP_REDUCE_PORTABLE_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.MAP_REDUCE_PORTABLE_FACTORY_ID;

/**
 * This class registers all Portable serializers that are needed for communication between nodes and clients
 */
public class MapReducePortableHook implements PortableHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(MAP_REDUCE_PORTABLE_FACTORY, MAP_REDUCE_PORTABLE_FACTORY_ID);
    public static final int TRANSFERABLE_PROCESS_INFORMATION = 4;

    private static final int LENGTH = TRANSFERABLE_PROCESS_INFORMATION + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public PortableFactory createFactory() {
        return new PortableFactory() {

            @SuppressWarnings("unchecked")
            private final ConstructorFunction<Integer, Portable>[] constructors = new ConstructorFunction[LENGTH];

            {
                constructors[TRANSFERABLE_PROCESS_INFORMATION] = new ConstructorFunction<Integer, Portable>() {
                    @Override
                    public Portable createNew(Integer arg) {
                        return new TransferableJobProcessInformation();
                    }
                };
            }

            public Portable create(int classId) {
                return (classId > 0 && classId <= constructors.length) ? constructors[classId].createNew(classId) : null;
            }
        };
    }

    @Override
    public Collection<ClassDefinition> getBuiltinDefinitions() {
        return null;
    }
}
