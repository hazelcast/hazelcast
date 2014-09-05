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

package com.hazelcast.mapreduce.impl.operation;

import com.hazelcast.mapreduce.TopologyChangedException;
import com.hazelcast.mapreduce.TopologyChangedStrategy;
import com.hazelcast.mapreduce.impl.MapReduceDataSerializerHook;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.mapreduce.impl.task.JobSupervisor;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.mapreduce.TopologyChangedStrategy.CANCEL_RUNNING_OPERATION;
import static com.hazelcast.mapreduce.impl.operation.RequestPartitionResult.ResultState.CHECK_STATE_FAILED;
import static com.hazelcast.mapreduce.impl.operation.RequestPartitionResult.ResultState.NO_SUPERVISOR;
import static com.hazelcast.mapreduce.impl.operation.RequestPartitionResult.ResultState.SUCCESSFUL;

/**
 * This operation is used to request assignment for keys on the job owners node. The job owner
 * is the only node capable of assigning those reducing nodes to be aware of the number of reducers
 * per partition and also aware of possible topology changes while executing the job.
 */
public class KeysAssignmentOperation
        extends ProcessingOperation {

    private Set<Object> keys;
    private KeysAssignmentResult result;

    public KeysAssignmentOperation() {
    }

    public KeysAssignmentOperation(String name, String jobId, Set<Object> keys) {
        super(name, jobId);
        this.keys = keys;
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    public void run()
            throws Exception {
        MapReduceService mapReduceService = getService();
        JobSupervisor supervisor = mapReduceService.getJobSupervisor(getName(), getJobId());
        if (supervisor == null) {
            this.result = new KeysAssignmentResult(NO_SUPERVISOR, null);
            return;
        }

        Map<Object, Address> assignment = new HashMap<Object, Address>();

        // Precheck if still all members are available
        if (!supervisor.checkAssignedMembersAvailable()) {
            TopologyChangedStrategy tcs = supervisor.getConfiguration().getTopologyChangedStrategy();
            if (tcs == CANCEL_RUNNING_OPERATION) {
                Exception exception = new TopologyChangedException();
                supervisor.cancelAndNotify(exception);
                this.result = new KeysAssignmentResult(CHECK_STATE_FAILED, assignment);
                return;
                // TODO Not yet fully supported
            /* } else if (tcs == DISCARD_AND_RESTART) {
             *   supervisor.cancelNotifyAndRestart();
             */
            } else {
                Exception exception = new TopologyChangedException("Unknown or unsupported TopologyChangedStrategy");
                supervisor.cancelAndNotify(exception);
                this.result = new KeysAssignmentResult(CHECK_STATE_FAILED, assignment);
                return;
            }
        }

        for (Object key : keys) {
            Address address = supervisor.assignKeyReducerAddress(key);
            assignment.put(key, address);
        }
        this.result = new KeysAssignmentResult(SUCCESSFUL, assignment);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeInt(keys.size());
        for (Object key : keys) {
            out.writeObject(key);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        keys = new HashSet<Object>();
        for (int i = 0; i < size; i++) {
            keys.add(in.readObject());
        }
    }

    @Override
    public int getFactoryId() {
        return MapReduceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapReduceDataSerializerHook.KEYS_ASSIGNMENT_OPERATION;
    }

}
