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

package com.hazelcast.replicatedmap.impl.client;

import com.hazelcast.internal.serialization.PortableHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.internal.util.ConstructorFunction;

import java.util.Collection;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.REPLICATED_PORTABLE_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.REPLICATED_PORTABLE_FACTORY_ID;

/**
 * This class registers all Portable serializers that are needed for communication between nodes and clients
 */
@SuppressWarnings("checkstyle:anoninnerlength")
public class ReplicatedMapPortableHook implements PortableHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(REPLICATED_PORTABLE_FACTORY, REPLICATED_PORTABLE_FACTORY_ID);

    public static final int MAP_ENTRIES = 12;
    public static final int MAP_KEY_SET = 13;
    public static final int VALUES_COLLECTION = 14;
    public static final int MAP_ENTRY_EVENT = 18;

    private static final int LENGTH = MAP_ENTRY_EVENT + 1;

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
                constructors[MAP_ENTRIES] = arg -> new ReplicatedMapEntries();
                constructors[MAP_KEY_SET] = arg -> new ReplicatedMapKeys();
                constructors[VALUES_COLLECTION] = arg -> new ReplicatedMapValueCollection();
                constructors[MAP_ENTRY_EVENT] = arg -> new ReplicatedMapPortableEntryEvent();
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
