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

import com.hazelcast.collection.operations.clientv2.AddAllRequest;
import com.hazelcast.collection.operations.clientv2.ClearRequest;
import com.hazelcast.nio.serialization.*;

import java.util.Collection;

/**
 * @ali 5/9/13
 */
public class CollectionPortableHook implements PortableHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.COLLECTION_PORTABLE_FACTORY, -12);

    public static final int ADD_ALL = 1;
    public static final int CLEAR = 2;

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
                }
                return null;
            }
        };
    }

    public Collection<ClassDefinition> getBuiltinDefinitions() {
        return null;
    }
}
