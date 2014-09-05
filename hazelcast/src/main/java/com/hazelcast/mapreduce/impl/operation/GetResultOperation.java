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

import com.hazelcast.mapreduce.impl.AbstractJobTracker;
import com.hazelcast.mapreduce.impl.MapReduceDataSerializerHook;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.mapreduce.impl.task.JobSupervisor;

import java.util.Map;

/**
 * This operation is used to retrieve results from a remote node
 */
public class GetResultOperation
        extends ProcessingOperation {

    private volatile Map result;

    public GetResultOperation() {
    }

    public GetResultOperation(String name, String jobId) {
        super(name, jobId);
    }

    public Map getResult() {
        return result;
    }

    @Override
    public void run()
            throws Exception {

        MapReduceService mapReduceService = getService();
        JobSupervisor supervisor = mapReduceService.getJobSupervisor(getName(), getJobId());
        if (supervisor != null) {
            result = supervisor.getJobResults();

            // This is the final call so cleanup on all nodes that are not job owners
            if (!supervisor.isOwnerNode()) {
                mapReduceService.destroyJobSupervisor(supervisor);
                AbstractJobTracker jobTracker = (AbstractJobTracker) mapReduceService.getJobTracker(getName());
                jobTracker.unregisterTrackableJob(getJobId());
                jobTracker.unregisterMapCombineTask(getJobId());
                jobTracker.unregisterReducerTask(getJobId());
            }
        }
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    public int getFactoryId() {
        return MapReduceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapReduceDataSerializerHook.GET_RESULT_OPERATION;
    }
}
