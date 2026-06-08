/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl.ops;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.VectorValues;
import com.hazelcast.vector.impl.VectorCollectionSerializerHook;

public class PutIfAbsentOperation extends BasePutOwnerOperation {

    private transient VectorDocument<Data> oldValue;

    public PutIfAbsentOperation() {
    }

    public PutIfAbsentOperation(String vectorCollectionName, Data key, Data userValue, VectorValues vectorValues) {
        super(vectorCollectionName, key, userValue, vectorValues);
    }

    @Override
    public void run() {
        oldValue = storage.putIfAbsent(key, userValue, vectorValues);
    }

    @Override
    public Object getResponse() {
        return oldValue;
    }

    @Override
    public boolean shouldBackup() {
        // generate backup operation only if a change was made
        return oldValue == null && super.shouldBackup();
    }

    @Override
    public int getClassId() {
        return VectorCollectionSerializerHook.PUT_IF_ABSENT;
    }
}
