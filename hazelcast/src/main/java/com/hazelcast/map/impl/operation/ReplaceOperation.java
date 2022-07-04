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

public class ReplaceOperation extends BasePutOperation implements MutatingOperation {

    private boolean successful;

    public ReplaceOperation(String name, Data dataKey, Data value) {
        super(name, dataKey, value);
    }

    public ReplaceOperation() {
    }

    @Override
    protected void runInternal() {
        Object oldValue = recordStore.replace(dataKey, dataValue);
        this.oldValue = mapServiceContext.toData(oldValue);
        successful = oldValue != null;
    }

    @Override
    public boolean shouldBackup() {
        return successful && super.shouldBackup();
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
    public int getClassId() {
        return MapDataSerializerHook.REPLACE;
    }
}
