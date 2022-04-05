/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.jmx;

import com.hazelcast.config.Config;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.collection.IQueue;
import com.hazelcast.internal.jmx.suppliers.LocalQueueStatsSupplier;
import com.hazelcast.internal.jmx.suppliers.StatsSupplier;
import com.hazelcast.collection.LocalQueueStats;

/**
 * Management bean for {@link IQueue}
 */
@ManagedDescription("IQueue")
public class QueueMBean extends HazelcastMBean<IQueue> {

    private final LocalStatsDelegate<LocalQueueStats> localQueueStatsDelegate;

    protected QueueMBean(IQueue managedObject, ManagementService service) {
        super(managedObject, service);
        this.objectName = service.createObjectName("IQueue", managedObject.getName());
        StatsSupplier<LocalQueueStats> localQueueStatsSupplier = new LocalQueueStatsSupplier(managedObject);
        this.localQueueStatsDelegate = new LocalStatsDelegate<LocalQueueStats>(localQueueStatsSupplier, updateIntervalSec);
    }

    @ManagedAnnotation("localOwnedItemCount")
    @ManagedDescription("the number of owned items in this member.")
    public long getLocalOwnedItemCount() {
        return localQueueStatsDelegate.getLocalStats().getOwnedItemCount();
    }

    @ManagedAnnotation("localBackupItemCount")
    @ManagedDescription("the number of backup items in this member.")
    public long getLocalBackupItemCount() {
        return localQueueStatsDelegate.getLocalStats().getBackupItemCount();
    }

    @ManagedAnnotation("localMinAge")
    @ManagedDescription("the min age of the items in this member.")
    public long getLocalMinAge() {
        return localQueueStatsDelegate.getLocalStats().getMinAge();
    }

    @ManagedAnnotation("localMaxAge")
    @ManagedDescription("the max age of the items in this member.")
    public long getLocalMaxAge() {
        return localQueueStatsDelegate.getLocalStats().getMaxAge();
    }

    @ManagedAnnotation("localAverageAge")
    @ManagedDescription("the average age of the items in this member.")
    public long getLocalAverageAge() {
        return localQueueStatsDelegate.getLocalStats().getAverageAge();
    }

    @ManagedAnnotation("localOfferOperationCount")
    @ManagedDescription("the number of offer/put/add operations in this member")
    public long getLocalOfferOperationCount() {
        return localQueueStatsDelegate.getLocalStats().getOfferOperationCount();
    }

    @ManagedAnnotation("localRejectedOfferOperationCount")
    @ManagedDescription("the number of rejected offers in this member")
    public long getLocalRejectedOfferOperationCount() {
        return localQueueStatsDelegate.getLocalStats().getRejectedOfferOperationCount();
    }

    @ManagedAnnotation("localPollOperationCount")
    @ManagedDescription("the number of poll/take/remove operations in this member")
    public long getLocalPollOperationCount() {
        return localQueueStatsDelegate.getLocalStats().getPollOperationCount();
    }

    @ManagedAnnotation("localEmptyPollOperationCount")
    @ManagedDescription("number of null returning poll operations in this member")
    public long getLocalEmptyPollOperationCount() {
        return localQueueStatsDelegate.getLocalStats().getEmptyPollOperationCount();
    }

    @ManagedAnnotation("localOtherOperationsCount")
    @ManagedDescription("number of other operations in this member")
    public long getLocalOtherOperationsCount() {
        return localQueueStatsDelegate.getLocalStats().getOtherOperationsCount();
    }

    @ManagedAnnotation("localEventOperationCount")
    @ManagedDescription("number of event operations in this member")
    public long getLocalEventOperationCount() {
        return localQueueStatsDelegate.getLocalStats().getEventOperationCount();
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
