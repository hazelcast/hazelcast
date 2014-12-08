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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

/**
 * @author enesakar 1/17/13
 */
public final class RemoveOperation extends BaseRemoveOperation implements IdentifiedDataSerializable {

    boolean successful;

    public RemoveOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    public RemoveOperation() {
    }

    public void run() {
        dataOldValue = mapService.getMapServiceContext().toData(recordStore.remove(dataKey));
        successful = dataOldValue != null;
    }

    public void afterRun() {
        if (successful) {
            super.afterRun();
        }
    }

    public boolean shouldBackup() {
        return successful;
    }

    @Override
    public String toString() {
        return "RemoveOperation{" + name + "}";
    }

    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    public int getId() {
        return MapDataSerializerHook.REMOVE;
    }
}
