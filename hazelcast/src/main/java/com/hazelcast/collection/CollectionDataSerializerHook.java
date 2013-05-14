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

package com.hazelcast.collection;

import com.hazelcast.collection.operations.*;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

/**
 * @ali 1/7/13
 */
//TODO register
public class CollectionDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.COLLECTION_DS_FACTORY, -12);

    public static final int ADD_ALL_BACKUP = 1;
    public static final int ADD_ALL = 2;
    public static final int CLEAR_BACKUP = 3;
    public static final int CLEAR = 4;
    public static final int COMPARE_AND_REMOVE_BACKUP = 5;
    public static final int COMPARE_AND_REMOVE = 6;
    public static final int CONTAINS_ALL = 7;
    public static final int CONTAINS_ENTRY = 8;
    public static final int CONTAINS = 9;
    public static final int COUNT = 10;
    public static final int ENTRY_SET = 11;
    public static final int GET_ALL = 12;
    public static final int GET = 13;
    public static final int INDEX_OF = 14;
    public static final int KEY_SET = 15;
    public static final int PUT_BACKUP = 16;
    public static final int PUT = 17;
    public static final int REMOVE_ALL_BACKUP = 18;
    public static final int REMOVE_ALL = 19;
    public static final int REMOVE_BACKUP = 20;
    public static final int REMOVE = 21;
    public static final int REMOVE_INDEX_BACKUP = 22;
    public static final int REMOVE_INDEX = 23;
    public static final int SET_BACKUP = 24;
    public static final int SET = 25;
    public static final int SIZE = 26;
    public static final int VALUES = 27;


    public int getFactoryId() {
        return F_ID;
    }

    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {
            public IdentifiedDataSerializable create(int typeId) {
                switch (typeId) {
                    case ADD_ALL_BACKUP:
                        return new AddAllBackupOperation();
                    case ADD_ALL:
                        return new AddAllOperation();
                    case CLEAR_BACKUP:
                        return new ClearBackupOperation();
                    case CLEAR:
                        return new ClearOperation();
                    case COMPARE_AND_REMOVE_BACKUP:
                        return new CompareAndRemoveBackupOperation();
                    case COMPARE_AND_REMOVE:
                        return new CompareAndRemoveOperation();
                    case CONTAINS_ALL:
                        return new ContainsAllOperation();
                    case CONTAINS_ENTRY:
                        return new ContainsEntryOperation();
                    case CONTAINS:
                        return new ContainsOperation();
                    case COUNT:
                        return new CountOperation();
                    case ENTRY_SET:
                        return new EntrySetOperation();
                    case GET_ALL:
                        return new GetAllOperation();
                    case GET:
                        return new GetOperation();
                    case INDEX_OF:
                        return new IndexOfOperation();
                    case KEY_SET:
                        return new KeySetOperation();
                    case PUT_BACKUP:
                        return new PutBackupOperation();
                    case PUT:
                        return new PutOperation();
                    case REMOVE_ALL_BACKUP:
                        return new RemoveAllBackupOperation();
                    case REMOVE_ALL:
                        return new RemoveAllOperation();
                    case REMOVE_BACKUP:
                        return new RemoveBackupOperation();
                    case REMOVE:
                        return new RemoveOperation();
                    case REMOVE_INDEX_BACKUP:
                        return new RemoveIndexBackupOperation();
                    case REMOVE_INDEX:
                        return new RemoveIndexOperation();
                    case SET_BACKUP:
                        return new SetBackupOperation();
                    case SET:
                        return new SetOperation();
                    case SIZE:
                        return new SizeOperation();
                    case VALUES:
                        return new ValuesOperation();

                }
                return null;
            }
        };
    }
}
