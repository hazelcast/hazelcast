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

package com.hazelcast.mapreduce.aggregation.impl;

import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableHook;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collection;

/**
 * This class registers all Portable serializers that are needed for communication between nodes and clients
 */
//CHECKSTYLE:OFF
public class AggregationsPortableHook
        implements PortableHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.AGGREGATIONS_PORTABLE_FACTORY, -24);

    private static final int EXECUTE_SUM_AGGREGATION = 1;
    private static final int LENGTH = EXECUTE_SUM_AGGREGATION + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public PortableFactory createFactory() {
        return new PortableFactory() {
            final ConstructorFunction<Integer, Portable> constructors[] = new ConstructorFunction[LENGTH];

            {
                constructors[EXECUTE_SUM_AGGREGATION] = new ConstructorFunction<Integer, Portable>() {
                    @Override
                    public Portable createNew(Integer arg) {
                        return null;
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
//CHECKSTYLE:ON
