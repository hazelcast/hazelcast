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

import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import java.io.IOException;

public class ReplaceIfSameOperation extends BasePutOperation {

    private Data testValue;
    private boolean successful = false;

    public ReplaceIfSameOperation(String name, Data dataKey, Data testValue, Data value) {
        super(name, dataKey, value);
        this.testValue = testValue;
    }

    public ReplaceIfSameOperation() {
    }

    public void run() {
        successful = recordStore.replace(dataKey, testValue, dataValue);
    }

    public void afterRun() {
        if (successful) {
            super.afterRun();
        }
    }

    public Object getResponse() {
        return successful;
    }

    public boolean shouldBackup() {
        return successful;
    }

    @Override
    public void onWaitExpire() {
        getResponseHandler().sendResponse(false);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        IOUtil.writeNullableData(out, testValue);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        testValue = IOUtil.readNullableData(in);
    }

    @Override
    public String toString() {
        return "ReplaceIfSameOperation{" + name + "}";
    }
}
