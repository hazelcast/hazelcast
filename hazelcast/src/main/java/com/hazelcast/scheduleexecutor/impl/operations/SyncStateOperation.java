/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.scheduleexecutor.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.scheduleexecutor.impl.ScheduledExecutorDataSerializerHook;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Thomas Kountis.
 */
public class SyncStateOperation
        extends AbstractSchedulerOperation {

    private String taskName;

    private Map state;

    public SyncStateOperation() {
    }

    public SyncStateOperation(String schedulerName, String taskName, Map state) {
        super(schedulerName);
        this.taskName = taskName;
        this.state = state;
    }

    @Override
    public void run()
            throws Exception {
        getContainer().syncState(taskName, state);
    }

    @Override
    public int getId() {
        return ScheduledExecutorDataSerializerHook.SYNC_STATE_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeUTF(taskName);
        out.writeInt(state.size());
        Iterator it = state.keySet().iterator();
        while (it.hasNext()) {
            Object key = it.next();
            out.writeObject(key);
            out.writeObject(state.get(key));
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        this.taskName = in.readUTF();
        int stateSize = in.readInt();
        this.state = new HashMap(stateSize);
        for (int i = 0; i < stateSize; i++) {
            this.state.put(in.readObject(), in.readObject());
        }
    }
}
