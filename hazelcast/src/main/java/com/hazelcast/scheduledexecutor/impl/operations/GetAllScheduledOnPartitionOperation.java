/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.scheduledexecutor.impl.operations;

import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;
import com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService;
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorDataSerializerHook;
import com.hazelcast.spi.ReadonlyOperation;

import java.util.ArrayList;
import java.util.List;

public class GetAllScheduledOnPartitionOperation
        extends AbstractGetAllScheduledOperation implements ReadonlyOperation {

    private List<ScheduledTaskHandler> response;

    public GetAllScheduledOnPartitionOperation() {
    }

    public GetAllScheduledOnPartitionOperation(String schedulerName) {
        super(schedulerName);
    }

    @Override
    public void run()
            throws Exception {
        List<ScheduledTaskHandler> handlers = new ArrayList<ScheduledTaskHandler>();
        DistributedScheduledExecutorService service = getService();

        populateScheduledForHolder(handlers, service, getPartitionId());
        response = handlers;
    }

    @Override
    public List<ScheduledTaskHandler> getResponse() {
        return response;
    }

    @Override
    public int getId() {
        return ScheduledExecutorDataSerializerHook.GET_ALL_SCHEDULED_ON_PARTITION;
    }

}
