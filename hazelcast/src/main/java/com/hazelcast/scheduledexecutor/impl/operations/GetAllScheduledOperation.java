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

import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;
import com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService;
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorContainer;
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorContainerHolder;
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorDataSerializerHook;
import com.hazelcast.scheduledexecutor.impl.ScheduledTaskDescriptor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class GetAllScheduledOperation
        extends AbstractSchedulerOperation {

    private static final int MEMBER_BIN = -1;

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
            populateScheduledForHolder(handlers, service, i);
        }

        // Member bin
        populateScheduledForHolder(handlers, service, MEMBER_BIN);
        response = handlers;
    }

    private void populateScheduledForHolder(List<ScheduledTaskHandler> handlers, DistributedScheduledExecutorService service,
                                            int holderId) {
        ScheduledExecutorContainerHolder partition = service.getPartitionOrMemberBin(holderId);
        Collection<ScheduledExecutorContainer> containers = partition.getContainers();
        for (ScheduledExecutorContainer container : containers) {
            Collection<ScheduledTaskDescriptor> tasks = container.getTasks();
            for (ScheduledTaskDescriptor task : tasks) {
                if (task.isMasterReplica()) {
                    handlers.add(container.offprintHandler(task.getDefinition().getName()));
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

}
