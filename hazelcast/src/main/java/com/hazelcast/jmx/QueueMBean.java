/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jmx;

import com.hazelcast.config.Config;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.IQueue;

/**
 * Management bean for {@link com.hazelcast.core.IQueue}
 */
@ManagedDescription("IQueue")
public class QueueMBean extends HazelcastMBean<IQueue> {

    protected QueueMBean(IQueue managedObject, ManagementService service) {
        super(managedObject, service);
        objectName = service.createObjectName("IQueue", managedObject.getName());
    }

    @ManagedAnnotation("localOwnedItemCount")
    @ManagedDescription("the number of owned items in this member.")
    public long getLocalOwnedItemCount() {
        return managedObject.getLocalQueueStats().getOwnedItemCount();
    }

    @ManagedAnnotation("localBackupItemCount")
    @ManagedDescription("the number of backup items in this member.")
    public long getLocalBackupItemCount() {
        return managedObject.getLocalQueueStats().getBackupItemCount();
    }

    @ManagedAnnotation("localMinAge")
    @ManagedDescription("the min age of the items in this member.")
    public long getLocalMinAge() {
        return managedObject.getLocalQueueStats().getMinAge();
    }

    @ManagedAnnotation("localMaxAge")
    @ManagedDescription("the max age of the items in this member.")
    public long getLocalMaxAge() {
        return managedObject.getLocalQueueStats().getMaxAge();
    }

    @ManagedAnnotation("localAvgAge")
    @ManagedDescription("the average age of the items in this member.")
    public long getLocalAvgAge() {
        return managedObject.getLocalQueueStats().getAvgAge();
    }

    @ManagedAnnotation("localOfferOperationCount")
    @ManagedDescription("the number of offer/put/add operations in this member")
    public long getLocalOfferOperationCount() {
        return managedObject.getLocalQueueStats().getOfferOperationCount();
    }

    @ManagedAnnotation("localRejectedOfferOperationCount")
    @ManagedDescription("the number of rejected offers in this member")
    public long getLocalRejectedOfferOperationCount() {
        return managedObject.getLocalQueueStats().getRejectedOfferOperationCount();
    }

    @ManagedAnnotation("localPollOperationCount")
    @ManagedDescription("the number of poll/take/remove operations in this member")
    public long getLocalPollOperationCount() {
        return managedObject.getLocalQueueStats().getPollOperationCount();
    }

    @ManagedAnnotation("localEmptyPollOperationCount")
    @ManagedDescription("number of null returning poll operations in this member")
    public long getLocalEmptyPollOperationCount() {
        return managedObject.getLocalQueueStats().getEmptyPollOperationCount();
    }

    @ManagedAnnotation("localOtherOperationsCount")
    @ManagedDescription("number of other operations in this member")
    public long getLocalOtherOperationsCount() {
        return managedObject.getLocalQueueStats().getOtherOperationsCount();
    }

    @ManagedAnnotation("localEventOperationCount")
    @ManagedDescription("number of event operations in this member")
    public long getLocalEventOperationCount() {
        return managedObject.getLocalQueueStats().getEventOperationCount();
    }

    @ManagedAnnotation("name")
    @ManagedDescription("Name of the DistributedObject")
    public String getName() {
        return managedObject.getName();
    }

    @ManagedAnnotation("partitionKey")
    @ManagedDescription("the partitionKey")
    public String getPartitionKey() {
        return managedObject.getPartitionKey();
    }

    @ManagedAnnotation("config")
    @ManagedDescription("QueueConfig")
    public String getConfig() {
        String managedObjectName = managedObject.getName();
        Config config = service.instance.getConfig();
        QueueConfig queueConfig = config.findQueueConfig(managedObjectName);
        return queueConfig.toString();
    }

    @ManagedAnnotation(value = "clear", operation = true)
    @ManagedDescription("Clear Queue")
    public void clear() {
        managedObject.clear();
    }
}
