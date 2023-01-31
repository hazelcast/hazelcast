/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.operation.steps.RemoveIfSameOpSteps;
import com.hazelcast.map.impl.operation.steps.engine.Step;
import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class RemoveIfSameOperation extends BaseRemoveOperation {

    private Data expect;
    private boolean successful;

    public RemoveIfSameOperation() {
    }

    public RemoveIfSameOperation(String name, Data dataKey, Data value) {
        super(name, dataKey);
        expect = value;
    }

    @Override
    protected void runInternal() {
        successful = recordStore.remove(dataKey, expect);
    }

    @Override
    public void afterRunInternal() {
        if (successful) {
            dataOldValue = expect;
            super.afterRunInternal();
        }
    }

    @Override
    public State createState() {
        return super.createState()
                .setExpect(expect);
    }

    @Override
    public void applyState(State state) {
        super.applyState(state);

        successful = ((Boolean) state.getResult());
        if (successful) {
            dataOldValue = getOldValue(state);
        }
    }

    @Override
    protected Data getOldValue(State state) {
        if (successful) {
            dataOldValue = mapServiceContext.toData(state.getOldValue());
        }
        return null;
    }

    @Override
    public Step getStartingStep() {
        return RemoveIfSameOpSteps.READ;
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
        IOUtil.writeData(out, expect);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        expect = IOUtil.readData(in);
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.REMOVE_IF_SAME;
    }
}
