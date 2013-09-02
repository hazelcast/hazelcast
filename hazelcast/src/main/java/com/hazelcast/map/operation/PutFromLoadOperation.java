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

public class PutFromLoadOperation extends BasePutOperation {

    public PutFromLoadOperation(String name, Data dataKey, Data value, long ttl) {
        super(name, dataKey, value, ttl);
    }

    public PutFromLoadOperation() {
    }

    public void run() {
        recordStore.putTransient(dataKey, dataValue, ttl);
    }

    @Override
    public boolean returnsResponse(Throwable throwable) {
        return true;
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    public void onWaitExpire() {
        getResponseHandler().sendResponse(true);
    }

    @Override
    public String toString() {
        return "PutFromLoadOperation{" + name + "}";
    }

}
