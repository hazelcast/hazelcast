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

package com.hazelcast.mapreduce.impl;

import com.hazelcast.mapreduce.impl.client.ClientCancellationRequest;
import com.hazelcast.mapreduce.impl.client.ClientJobProcessInformationRequest;
import com.hazelcast.mapreduce.impl.client.ClientMapReduceRequest;
import com.hazelcast.mapreduce.impl.task.TransferableJobProcessInformation;
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
public class MapReducePortableHook
        implements PortableHook {

    //CHECKSTYLE:OFF
    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.MAP_REDUCE_PORTABLE_FACTORY, -23);

    public static final int CLIENT_JOB_PROCESS_INFO_REQUEST = 1;
    public static final int CLIENT_CANCELLATION_REQUEST = 2;
    public static final int CLIENT_MAP_REDUCE_REQUEST = 3;
    public static final int TRANSFERABLE_PROCESS_INFORMATION = 4;
    //CHECKSTYLE:ON

    private static final int LENGTH = TRANSFERABLE_PROCESS_INFORMATION + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    //CHECKSTYLE:OFF
    @Override
    public PortableFactory createFactory() {
        return new PortableFactory() {
            private final ConstructorFunction<Integer, Portable> constructors[] = new ConstructorFunction[LENGTH];

            {
                constructors[CLIENT_JOB_PROCESS_INFO_REQUEST] = new ConstructorFunction<Integer, Portable>() {
                    @Override
                    public Portable createNew(Integer arg) {
                        return new ClientJobProcessInformationRequest();
                    }
                };
                constructors[CLIENT_CANCELLATION_REQUEST] = new ConstructorFunction<Integer, Portable>() {
                    @Override
                    public Portable createNew(Integer arg) {
                        return new ClientCancellationRequest();
                    }
                };
                constructors[CLIENT_MAP_REDUCE_REQUEST] = new ConstructorFunction<Integer, Portable>() {
                    @Override
                    public Portable createNew(Integer arg) {
                        return new ClientMapReduceRequest();
                    }
                };
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
    //CHECKSTYLE:ON

    @Override
    public Collection<ClassDefinition> getBuiltinDefinitions() {
        return null;
    }

}
