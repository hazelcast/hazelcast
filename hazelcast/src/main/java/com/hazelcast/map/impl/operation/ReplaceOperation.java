/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

public class ReplaceOperation extends BasePutOperation {

    private boolean successful;

    public ReplaceOperation(String name, Data dataKey, Data value) {
        super(name, dataKey, value);
    }

    public ReplaceOperation() {
    }

    @Override
    public void run() {
        Object oldValue = recordStore.replace(dataKey, dataValue);
        dataOldValue = mapServiceContext.toData(oldValue);
        successful = oldValue != null;
    }

    @Override
    public boolean shouldBackup() {
        return successful && recordStore.getRecord(dataKey) != null;
    }

    @Override
    public void afterRun() {
        if (successful) {
            super.afterRun();
        }
    }


    @Override
    public Object getResponse() {
        return dataOldValue;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.REPLACE;
    }
}
