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

import com.hazelcast.map.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

//todo: unused class?
public class MergeRemoveOperation extends BaseRemoveOperation {

    private long removeTime;
    private boolean merged = false;

    public MergeRemoveOperation(String name, Data dataKey, long removeTime) {
        super(name, dataKey);
        this.removeTime = removeTime;
    }

    public MergeRemoveOperation() {
    }

    public void run() {
        Record record = recordStore.getRecord(dataKey);
        // todo what if statistics is disabled. currently it accepts the remove
        // check if there is newer update or insert. If so then cancel the remove
        if (record.getCreationTime() > removeTime
                || record.getLastUpdateTime() > removeTime) {
            return;
        }
        recordStore.deleteRecord(dataKey);
        merged = true;
    }

    @Override
    public Object getResponse() {
        return merged;
    }

    public void afterRun() {
        if (merged) {
            invalidateNearCaches();
        }
    }

    public boolean shouldBackup() {
        return merged;
    }

    @Override
    public void onWaitExpire() {
        getResponseHandler().sendResponse(false);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(removeTime);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        removeTime = in.readLong();
    }

    @Override
    public String toString() {
        return "MergeRemoveOperation{" + name + "}";
    }

}
