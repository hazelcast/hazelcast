/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.nio.serialization.Data;

public class WanOriginatedDeleteOperation extends BaseRemoveOperation {

    private boolean success;

    public WanOriginatedDeleteOperation() {
    }

    public WanOriginatedDeleteOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    @Override
    public void run() {
        success = recordStore.remove(dataKey) != null;
    }

    @Override
    public Object getResponse() {
        return success;
    }

    @Override
    public void afterRun() {
        if (!success) {
            return;
        }

        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        mapServiceContext.interceptAfterRemove(name, dataValue);
        mapServiceContext.getMapEventPublisher()
                .publishEvent(getCallerAddress(), name, EntryEventType.REMOVED, dataKey, dataOldValue, null);
        invalidateNearCaches();
        evict(false);
    }

    @Override
    public boolean shouldBackup() {
        return success;
    }

    @Override
    public void onWaitExpire() {
        sendNormalResponse(false);
    }

    @Override
    public String toString() {
        return "DeleteOperation{" + name + "}";
    }

}
