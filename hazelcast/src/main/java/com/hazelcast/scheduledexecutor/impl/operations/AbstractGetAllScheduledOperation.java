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

package com.hazelcast.scheduledexecutor.impl.operations;

import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;
import com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService;
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorContainer;
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorContainerHolder;
import com.hazelcast.scheduledexecutor.impl.ScheduledTaskDescriptor;

import java.util.Collection;
import java.util.List;

public abstract class AbstractGetAllScheduledOperation
        extends AbstractSchedulerOperation {

    public AbstractGetAllScheduledOperation() {
    }

    public AbstractGetAllScheduledOperation(String schedulerName) {
        super(schedulerName);
    }

    protected void populateScheduledForHolder(List<ScheduledTaskHandler> handlers, DistributedScheduledExecutorService service,
                                              int holderId) {
        ScheduledExecutorContainerHolder partition = service.getPartitionOrMemberBin(holderId);
        ScheduledExecutorContainer container = partition.getContainer(schedulerName);
        if (container == null || service.isShutdown(schedulerName)) {
            return;
        }

        Collection<ScheduledTaskDescriptor> tasks = container.getTasks();
        for (ScheduledTaskDescriptor task : tasks) {
            handlers.add(container.offprintHandler(task.getDefinition().getName()));
        }
    }
}
