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

import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;

import java.util.HashMap;
import java.util.Map;

/**
 * @ali 1/7/13
 */
public class DataSerializerCollectionHook implements DataSerializerHook {

    static final int COLLECTION_OPERATION = 400;
    static final int COLLECTION_BACKUP_OPERATION = 401;

    public Map<Integer, DataSerializableFactory> getFactories() {
        final Map<Integer, DataSerializableFactory> factories = new HashMap<Integer, DataSerializableFactory>();
        factories.put(COLLECTION_OPERATION, new DataSerializableFactory() {
            public DataSerializable create() {
                return new CollectionOperation();
            }
        });
        factories.put(COLLECTION_BACKUP_OPERATION, new DataSerializableFactory() {
            public DataSerializable create() {
                return new CollectionBackupOperation();
            }
        });
        return factories;
    }
}
