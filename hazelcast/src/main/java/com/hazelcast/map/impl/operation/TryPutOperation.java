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

import com.hazelcast.nio.serialization.Data;

public class TryPutOperation extends BasePutOperation {

    private boolean successful;

    public TryPutOperation(String name, Data dataKey, Data value, long timeout) {
        super(name, dataKey, value);
        setWaitTimeout(timeout);
    }

    public TryPutOperation() {
    }

    public void run() {
        successful = recordStore.tryPut(dataKey, dataValue, ttl);
    }

    public void afterRun() {
        if (successful) {
            super.afterRun();
        }
    }

    public boolean shouldBackup() {
        return successful;
    }

    public void onWaitExpire() {
        getResponseHandler().sendResponse(false);
    }

    public Object getResponse() {
        return successful;
    }

    @Override
    public String toString() {
        return "TryPutOperation{" + name + "}";
    }
}
