/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

public class PutIfAbsentOperation extends BasePutOperation implements MutatingOperation {

    private boolean successful;

    public PutIfAbsentOperation(String name, Data dataKey, Data value, long ttl, long maxIdle) {
        super(name, dataKey, value, ttl, maxIdle);
    }

    public PutIfAbsentOperation() {
    }

    @Override
    protected void runInternal() {
        final Object oldValue = recordStore.putIfAbsent(dataKey, dataValue, ttl, maxIdle, getCallerAddress());
        this.oldValue = mapServiceContext.toData(oldValue);
        successful = this.oldValue == null;
    }

    @Override
    protected void afterRunInternal() {
        if (successful) {
            super.afterRunInternal();
        }
    }

    @Override
    public Object getResponse() {
        return oldValue;
    }

    @Override
    public boolean shouldBackup() {
        return successful && recordStore.getRecord(dataKey) != null;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.PUT_IF_ABSENT;
    }
}
