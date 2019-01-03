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

package com.hazelcast.map.impl.client;

import com.hazelcast.internal.serialization.PortableHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;

import java.util.Collection;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.MAP_PORTABLE_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.MAP_PORTABLE_FACTORY_ID;

/**
 * Portable serialization hook for Map structures.
 */
public class MapPortableHook implements PortableHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(MAP_PORTABLE_FACTORY, MAP_PORTABLE_FACTORY_ID);

    public static final int CREATE_ACCUMULATOR_INFO = 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public PortableFactory createFactory() {
        return new MapPortableFactory();
    }

    @Override
    public Collection<ClassDefinition> getBuiltinDefinitions() {
        return null;
    }

    /**
     * Implements {@link PortableFactory} for this {@link PortableHook}.
     */
    private static class MapPortableFactory implements PortableFactory {

        /**
         * Creates a Portable instance using given class ID.
         *
         * @param classId portable class ID
         * @return portable instance or null if class ID is not known by this factory
         */
        @Override
        public Portable create(int classId) {
            if (classId == CREATE_ACCUMULATOR_INFO) {
                return new AccumulatorInfo();
            }
            throw new IndexOutOfBoundsException("No registered constructor exists with class ID: " + classId);
        }
    }
}
