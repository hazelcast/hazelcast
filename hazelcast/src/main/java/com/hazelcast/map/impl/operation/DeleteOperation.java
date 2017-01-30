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

public class DeleteOperation extends BaseRemoveOperation {
    private boolean success;

    public DeleteOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    public DeleteOperation() {
    }

    @Override
    public void run() {
        success = recordStore.delete(dataKey);
    }

    @Override
    public Object getResponse() {
        return success;
    }

    @Override
    public void afterRun() {
        if (success) {
            super.afterRun();
        }
    }

    @Override
    public boolean shouldBackup() {
        return success;
    }

    @Override
    public void onWaitExpire() {
        sendResponse(false);
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.DELETE;
    }
}
