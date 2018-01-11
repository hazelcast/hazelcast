/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

public class RemoveIfSameOperation extends BaseRemoveOperation {

    private Data testValue;
    private boolean successful;

    public RemoveIfSameOperation() {
    }

    public RemoveIfSameOperation(String name, Data dataKey, Data value) {
        super(name, dataKey);
        testValue = value;
    }

    @Override
    public void run() {
        successful = recordStore.remove(dataKey, testValue);
    }

    @Override
    public void afterRun() {
        if (successful) {
            dataOldValue = testValue;
            super.afterRun();
        }
    }

    @Override
    public Object getResponse() {
        return successful;
    }

    @Override
    public boolean shouldBackup() {
        return successful;
    }

    @Override
    public void onWaitExpire() {
        sendResponse(null);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(testValue);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        testValue = in.readData();
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.REMOVE_IF_SAME;
    }
}
