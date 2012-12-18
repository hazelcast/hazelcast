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

import com.hazelcast.nio.Data;
import com.hazelcast.nio.IOUtil;

import java.io.DataInput;
import java.io.DataOutput;
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

    public void beforeRun() {
        init();
    }

    public void doOp() {
        if (prepareTransaction()) {
            return;
        }
        prepareValue();
        if (record != null && record.getValue().equals(IOUtil.toObject(testValue))) {
            remove();
            store();
            removed = true;
        }
    }

    @Override
    public void writeInternal(DataOutput out) throws IOException {
        super.writeInternal(out);
        IOUtil.writeNullableData(out, testValue);
    }

    @Override
    public void readInternal(DataInput in) throws IOException {
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
    public String toString() {
        return "RemoveIfSameOperation{" + name + "}";
    }
}
