/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.monitor.client.event;

import java.io.Serializable;
import java.util.Collection;

public class QueueStatistics extends InstanceStatistics {

    Collection<LocalQueueStatistics> list;

    public QueueStatistics(int clusterId) {
        super(clusterId);
    }

    @Override
    public Collection<? extends LocalInstanceStatistics> getListOfLocalStats() {
        return list;
    }

    public QueueStatistics() {
    }

    public ChangeEventType getChangeEventType() {
        return ChangeEventType.QUEUE_STATISTICS;
    }

    public Collection<LocalQueueStatistics> getList() {
        return list;
    }

    public void setList(Collection<LocalQueueStatistics> list) {
        this.list = list;
        for (LocalQueueStatistics queueStatistics : list) {
            totalOPS = totalOPS + (int) (queueStatistics.numberOfOffersInSec + queueStatistics.numberOfPollsInSec);
            size = size + queueStatistics.ownedItemCount;
        }
    }

    public static class LocalQueueStatistics implements Serializable, LocalInstanceStatistics {
        public int ownedItemCount;
        public int backupItemCount;
        public long minAge;
        public long maxAge;
        public long aveAge;
        public long periodStart;
        public long periodEnd;
        public long numberOfOffersInSec;
        public long numberOfRejectedOffersInSec;
        public long numberOfPollsInSec;
        public long numberOfEmptyPollsInSec;
        public String memberName;
    }

    @Override
    public String toString() {
        return "QueueStatistics" + super.toString();
    }
}
