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

package com.hazelcast.map.operation;

import com.hazelcast.nio.serialization.Data;

public class ReplaceOperation extends BasePutOperation {

    private boolean successful = false;

    public ReplaceOperation(String name, Data dataKey, Data value) {
        super(name, dataKey, value);
    }

    public ReplaceOperation() {
    }

    public void run() {
        final Object oldValue = recordStore.replace(dataKey, dataValue);
        dataOldValue = mapService.getMapServiceContext().toData(oldValue);
        successful = oldValue != null;
    }

    public boolean shouldBackup() {
        return successful;
    }

    public void afterRun() {
        if (successful) {
            super.afterRun();
        }
    }

    @Override
    public String toString() {
        return "ReplaceOperation{" + name + "}";
    }

    @Override
    public Object getResponse() {
        return dataOldValue;
    }
}
