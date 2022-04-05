/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;

import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.UPDATED;
import static com.hazelcast.map.impl.record.Record.UNSET;

public class SetOperation extends BasePutOperation implements MutatingOperation {

    private transient boolean newRecord;

    public SetOperation() {
    }

    public SetOperation(String name, Data dataKey, Data value) {
        super(name, dataKey, value);
    }

    @Override
    protected void runInternal() {
        oldValue = recordStore.set(dataKey, dataValue, getTtl(), getMaxIdle());
        newRecord = oldValue == null;
    }

    protected long getTtl() {
        return UNSET;
    }

    protected long getMaxIdle() {
        return UNSET;
    }

    @Override
    protected void afterRunInternal() {
        eventType = newRecord ? ADDED : UPDATED;

        super.afterRunInternal();
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.SET;
    }
}
