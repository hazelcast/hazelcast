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

package com.hazelcast.map;

import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

public class RemoveIfSameOperation extends BaseRemoveOperation {

    Data testValue;
    boolean removed = false;

    public RemoveIfSameOperation(String name, Data dataKey, Data oldValue, String txnId) {
        super(name, dataKey, txnId);
        testValue = oldValue;
    }

    public RemoveIfSameOperation() {
    }

    public void run() {
        if (prepareTransaction()) {
            return;
        }
       removed = recordStore.remove(dataKey, testValue);
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

    public Object getResponse() {
        return removed;
    }

    public boolean shouldBackup() {
        return removed;
    }


    @Override
    public void onWaitExpire() {
        getResponseHandler().sendResponse(null);
    }


    @Override
    public String toString() {
        return "RemoveIfSameOperation{" + name + "}";
    }
}
