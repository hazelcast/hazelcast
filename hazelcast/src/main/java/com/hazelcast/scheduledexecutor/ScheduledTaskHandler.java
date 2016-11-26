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

package com.hazelcast.scheduledexecutor;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.scheduledexecutor.impl.ScheduledTaskHandlerImpl;
import com.hazelcast.spi.annotation.Beta;

@Beta
public abstract class ScheduledTaskHandler
        implements IdentifiedDataSerializable {

    public abstract Address getAddress();

    public abstract int getPartitionId();

    public abstract String getSchedulerName();

    public abstract String getTaskName();

    public abstract boolean isAssignedToPartition();

    public abstract boolean isAssignedToMember();

    public abstract String toUrn();

    public static ScheduledTaskHandler of(Address addr, String schedulerName, String taskName) {
        return ScheduledTaskHandlerImpl.of(addr, schedulerName, taskName);
    }

    public static ScheduledTaskHandler of(int partitionId, String schedulerName, String taskName) {
        return ScheduledTaskHandlerImpl.of(partitionId, schedulerName, taskName);
    }

    public static ScheduledTaskHandler of(String urn) {
        return ScheduledTaskHandlerImpl.of(urn);
    }

}
