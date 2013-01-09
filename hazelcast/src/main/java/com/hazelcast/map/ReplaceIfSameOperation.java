/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

public class ReplaceIfSameOperation extends BasePutOperation {

    Data testValue;
    boolean replaced = false;


    public ReplaceIfSameOperation(String name, Data dataKey, Data oldValue, Data value, String txnId) {
        super(name, dataKey, value, txnId);
        testValue = oldValue;
    }

    public ReplaceIfSameOperation() {
    }

    public void run() {
        if (prepareTransaction()) {
            return;
        }
        replaced = recordStore.replace(dataKey, testValue, dataValue);
    }

    public Object getResponse() {
        return replaced;
    }

    public boolean shouldBackup() {
        return replaced;
    }

    @Override
    public void onWaitExpire() {
        getResponseHandler().sendResponse(false);
    }


    @Override
    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        IOUtil.writeNullableData(out, testValue);
    }

    @Override
    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        testValue = IOUtil.readNullableData(in);
    }

    @Override
    public String toString() {
        return "ReplaceIfSameOperation{" + name + "}";
    }
}
