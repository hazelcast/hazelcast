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

package com.hazelcast.scheduledexecutor.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;
import com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService;
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorContainer;
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorDataSerializerHook;
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorPartition;
import com.hazelcast.scheduledexecutor.impl.ScheduledTaskDescriptor;
import com.hazelcast.scheduledexecutor.impl.ScheduledTaskHandlerImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by Thomas Kountis.
 */
public class GetAllScheduledOperation
        extends AbstractSchedulerOperation {

    private List<ScheduledTaskHandler> response;

    public GetAllScheduledOperation() {
    }

    public GetAllScheduledOperation(String schedulerName) {
        super(schedulerName);
    }

    @Override
    public void run()
            throws Exception {
        List<ScheduledTaskHandler> handlers = new ArrayList<ScheduledTaskHandler>();
        DistributedScheduledExecutorService service = getService();

        int partitionCount = getNodeEngine().getPartitionService().getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            populateScheduledForPartition(handlers, service, i);
        }

        // Member partition
        populateScheduledForPartition(handlers, service, -1);
        response = handlers;
    }

    private void populateScheduledForPartition(List<ScheduledTaskHandler> handlers, DistributedScheduledExecutorService service,
                                               int partitionId) {
        ScheduledExecutorPartition partition = service.getPartition(partitionId);
        Collection<ScheduledExecutorContainer> containers = partition.getContainers();
        for (ScheduledExecutorContainer container : containers) {
            Collection<ScheduledTaskDescriptor> tasks = container.getTasks();
            for (ScheduledTaskDescriptor task : tasks) {
                if (partitionId == -1) {
                    handlers.add(ScheduledTaskHandlerImpl.of(getNodeEngine().getThisAddress(),
                            getSchedulerName(), task.getDefinition().getName()));
                } else {
                    handlers.add(ScheduledTaskHandlerImpl.of(partitionId, getSchedulerName(), task.getDefinition().getName()));
                }
            }
        }
    }

    @Override
    public List<ScheduledTaskHandler> getResponse() {
        return response;
    }

    @Override
    public int getId() {
        return ScheduledExecutorDataSerializerHook.GET_ALL_SCHEDULED;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
    }
}
