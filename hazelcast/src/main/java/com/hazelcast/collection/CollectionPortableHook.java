/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection;

import com.hazelcast.collection.operations.client.*;
import com.hazelcast.nio.serialization.*;

import java.util.Collection;

/**
 * @ali 5/9/13
 */
public class CollectionPortableHook implements PortableHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.COLLECTION_PORTABLE_FACTORY, -12);

    public static final int ADD_ALL = 1;
    public static final int CLEAR = 2;
    public static final int COMPARE_AND_REMOVE = 3;
    public static final int CONTAINS_ALL = 4;
    public static final int CONTAINS_ENTRY = 5;
    public static final int CONTAINS = 6;
    public static final int COUNT = 7;
    public static final int ENTRY_SET = 8;
    public static final int GET_ALL = 9;
    public static final int GET = 10;
    public static final int INDEX_OF = 11;
    public static final int KEY_SET = 12;
    public static final int PUT = 13;
    public static final int REMOVE_ALL = 14;
    public static final int REMOVE_INDEX = 15;
    public static final int REMOVE = 16;
    public static final int SET = 17;
    public static final int SIZE = 18;
    public static final int VALUES = 19;
    public static final int ADD_LISTENER = 20;
    public static final int COLLECTION_RESPONSE = 21;
    public static final int ENTRY_SET_RESPONSE = 22;


    public int getFactoryId() {
        return F_ID;
    }

    public PortableFactory createFactory() {
        return new PortableFactory() {
            public Portable create(int classId) {
                switch (classId){
                    case ADD_ALL:
                        return new AddAllRequest();
                    case CLEAR:
                        return new ClearRequest();
                    case COMPARE_AND_REMOVE:
                        return new CompareAndRemoveRequest();
                    case CONTAINS_ALL:
                        return new ContainsAllRequest();
                    case CONTAINS_ENTRY:
                        return new ContainsEntryRequest();
                    case CONTAINS:
                        return new ContainsRequest();
                    case COUNT:
                        return new CountRequest();
                    case ENTRY_SET:
                        return new EntrySetRequest();
                    case GET_ALL:
                        return new GetAllRequest();
                    case GET:
                        return new GetRequest();
                    case INDEX_OF:
                        return new IndexOfRequest();
                    case KEY_SET:
                        return new KeySetRequest();
                    case PUT:
                        return new PutRequest();
                    case REMOVE_ALL:
                        return new RemoveAllRequest();
                    case REMOVE_INDEX:
                        return new RemoveIndexRequest();
                    case REMOVE:
                        return new RemoveRequest();
                    case SET:
                        return new SetRequest();
                    case SIZE:
                        return new SizeRequest();
                    case VALUES:
                        return new ValuesRequest();
                    case ADD_LISTENER:
                        return new AddListenerRequest();
                    case COLLECTION_RESPONSE:
                        return new PortableCollectionResponse();
                    case ENTRY_SET_RESPONSE:
                        return new PortableEntrySetResponse();
                }
                return null;
            }
        };
    }

    public Collection<ClassDefinition> getBuiltinDefinitions() {
        return null;
    }
}
