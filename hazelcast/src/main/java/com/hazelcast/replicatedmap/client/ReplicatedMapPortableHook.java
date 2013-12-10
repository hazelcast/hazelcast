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

package com.hazelcast.replicatedmap.client;

import com.hazelcast.nio.serialization.*;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collection;

public class ReplicatedMapPortableHook implements PortableHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.REPLICATED_PORTABLE_FACTORY, -22);

    public static final int SIZE = 0;
    public static final int IS_EMPTY = 1;
    public static final int CONTAINS_KEY = 2;
    public static final int CONTAINS_VALUE = 3;
    public static final int PUT_TTL = 4;
    public static final int GET = 5;
    public static final int REMOVE = 6;
    public static final int PUT_ALL = 7;
    public static final int KEY_SET = 8;
    public static final int VALUES = 9;
    public static final int ENTRY_SET = 10;
    public static final int MAP_ENTRY_SET = 11;
    public static final int GET_RESPONSE = 12;
    public static final int ADD_LISTENER = 13;
    public static final int REMOVE_LISTENER = 14;

    private static final int LENGTH = REMOVE_LISTENER + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public PortableFactory createFactory() {
        return new PortableFactory() {
            final ConstructorFunction<Integer, Portable> constructors[] = new ConstructorFunction[LENGTH];

            {
                constructors[SIZE] = new ConstructorFunction<Integer, Portable>() {
                    @Override
                    public Portable createNew(Integer arg) {
                        return new ClientReplicatedMapSizeRequest();
                    }
                };
                constructors[IS_EMPTY] = new ConstructorFunction<Integer, Portable>() {
                    @Override
                    public Portable createNew(Integer arg) {
                        return new ClientReplicatedMapIsEmptyRequest();
                    }
                };
                constructors[CONTAINS_KEY] = new ConstructorFunction<Integer, Portable>() {
                    @Override
                    public Portable createNew(Integer arg) {
                        return new ClientReplicatedMapContainsKeyRequest();
                    }
                };
                constructors[CONTAINS_VALUE] = new ConstructorFunction<Integer, Portable>() {
                    @Override
                    public Portable createNew(Integer arg) {
                        return new ClientReplicatedMapContainsValueRequest();
                    }
                };
                constructors[PUT_TTL] = new ConstructorFunction<Integer, Portable>() {
                    @Override
                    public Portable createNew(Integer arg) {
                        return new ClientReplicatedMapPutTtlRequest();
                    }
                };
                constructors[GET] = new ConstructorFunction<Integer, Portable>() {
                    @Override
                    public Portable createNew(Integer arg) {
                        return new ClientReplicatedMapGetRequest();
                    }
                };
                constructors[REMOVE] = new ConstructorFunction<Integer, Portable>() {
                    @Override
                    public Portable createNew(Integer arg) {
                        return new ClientReplicatedMapRemoveRequest();
                    }
                };
                constructors[PUT_ALL] = new ConstructorFunction<Integer, Portable>() {
                    @Override
                    public Portable createNew(Integer arg) {
                        return new ClientReplicatedMapPutAllRequest();
                    }
                };
                constructors[KEY_SET] = new ConstructorFunction<Integer, Portable>() {
                    @Override
                    public Portable createNew(Integer arg) {
                        return new ClientReplicatedMapKeySetRequest();
                    }
                };
                constructors[VALUES] = new ConstructorFunction<Integer, Portable>() {
                    @Override
                    public Portable createNew(Integer arg) {
                        return new ClientReplicatedMapValuesRequest();
                    }
                };
                constructors[ENTRY_SET] = new ConstructorFunction<Integer, Portable>() {
                    @Override
                    public Portable createNew(Integer arg) {
                        return new ClientReplicatedMapEntrySetRequest();
                    }
                };
                constructors[MAP_ENTRY_SET] = new ConstructorFunction<Integer, Portable>() {
                    @Override
                    public Portable createNew(Integer arg) {
                        return new ReplicatedMapEntrySet();
                    }
                };
                constructors[GET_RESPONSE] = new ConstructorFunction<Integer, Portable>() {
                    @Override
                    public Portable createNew(Integer arg) {
                        return new GetResponse();
                    }
                };
                constructors[ADD_LISTENER] = new ConstructorFunction<Integer, Portable>() {
                    @Override
                    public Portable createNew(Integer arg) {
                        return new ClientReplicatedMapAddEntryListenerRequest();
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
