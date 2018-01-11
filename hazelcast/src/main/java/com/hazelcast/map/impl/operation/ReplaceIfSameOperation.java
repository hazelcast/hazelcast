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
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;

public class ReplaceIfSameOperation extends BasePutOperation implements MutatingOperation {

    private Data expect;
    private boolean successful;

    public ReplaceIfSameOperation() {
    }

    public ReplaceIfSameOperation(String name, Data dataKey, Data expect, Data update) {
        super(name, dataKey, update);
        this.expect = expect;
    }

    @Override
    public void run() {
        successful = recordStore.replace(dataKey, expect, dataValue);
        if (successful) {
            dataOldValue = expect;
        }
    }

    @Override
    public void afterRun() {
        if (successful) {
            super.afterRun();
        }
    }

    @Override
    public Object getResponse() {
        return successful;
    }

    @Override
    public boolean shouldBackup() {
        return successful && recordStore.getRecord(dataKey) != null;
    }

    @Override
    public void onWaitExpire() {
        sendResponse(false);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(expect);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        expect = in.readData();
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.REPLACE_IF_SAME;
    }
}
