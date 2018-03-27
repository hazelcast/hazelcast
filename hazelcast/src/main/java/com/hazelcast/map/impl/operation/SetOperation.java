/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.MutatingOperation;

import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.UPDATED;

public class SetOperation extends BasePutOperation implements MutatingOperation {

    private boolean newRecord;

    public SetOperation() {
    }

    public SetOperation(String name, Data dataKey, Data value, long ttl) {
        super(name, dataKey, value, ttl);
    }

    @Override
    public void run() {
        Object oldValue = recordStore.set(dataKey, dataValue, ttl);
        newRecord = oldValue == null;

        if (recordStore.hasQueryCache()) {
            dataOldValue = mapServiceContext.toData(oldValue);
        }
    }

    @Override
    public void afterRun() {
        eventType = newRecord ? ADDED : UPDATED;

        super.afterRun();
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.SET;
    }
}
