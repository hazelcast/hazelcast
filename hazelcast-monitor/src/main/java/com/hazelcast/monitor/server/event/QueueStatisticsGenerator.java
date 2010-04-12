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

package com.hazelcast.monitor.server.event;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiTask;
import com.hazelcast.monitor.DistributedQueueStatsCallable;
import com.hazelcast.monitor.LocalQueueOperationStats;
import com.hazelcast.monitor.client.event.ChangeEvent;
import com.hazelcast.monitor.client.event.ChangeEventType;
import com.hazelcast.monitor.client.event.QueueStatistics;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

public class QueueStatisticsGenerator extends InstanceStatisticsGenerator {

    public QueueStatisticsGenerator(HazelcastClient client, String name, int clusterId) {
        super(name, client, clusterId);
    }

    public ChangeEvent generateEvent() {
        ExecutorService esService = client.getExecutorService();
        Set<Member> members = client.getCluster().getMembers();
        final List<Member> lsMembers = new ArrayList<Member>(members);
        MultiTask<DistributedQueueStatsCallable.MemberQueueStats> task =
                new MultiTask<DistributedQueueStatsCallable.MemberQueueStats>(new DistributedQueueStatsCallable(name), members);
        esService.execute(task);
        Collection<DistributedQueueStatsCallable.MemberQueueStats> queueStats;
        try {
            queueStats = task.get();
        } catch (InterruptedException e) {
            return null;
        } catch (ExecutionException e) {
            return null;
        }
        if (queueStats == null) {
            return null;
        }
        if (members.size() != queueStats.size()) {
            return null;
        }
        List<DistributedQueueStatsCallable.MemberQueueStats> lsQueueStats = new ArrayList(queueStats);
        Collections.sort(lsQueueStats, new Comparator<DistributedQueueStatsCallable.MemberQueueStats>() {
            public int compare(DistributedQueueStatsCallable.MemberQueueStats o1, DistributedQueueStatsCallable.MemberQueueStats o2) {
                int i1 = lsMembers.indexOf(o1.getMember());
                int i2 = lsMembers.indexOf(o2.getMember());
                return i1 - i2;
            }
        });
        List<QueueStatistics.LocalQueueStatistics> listOfStats = new ArrayList<QueueStatistics.LocalQueueStatistics>();
        for (DistributedQueueStatsCallable.MemberQueueStats memberQStat : lsQueueStats) {
            QueueStatistics.LocalQueueStatistics stat = new QueueStatistics.LocalQueueStatistics();
            stat.ownedItemCount = memberQStat.getLocalQueueStats().getOwnedItemCount();
            stat.backupItemCount = memberQStat.getLocalQueueStats().getBackupItemCount();
            stat.maxAge = memberQStat.getLocalQueueStats().getMaxAge();
            stat.minAge = memberQStat.getLocalQueueStats().getMinAge();
            stat.aveAge = memberQStat.getLocalQueueStats().getAveAge();
            stat.periodEnd = memberQStat.getLocalQueueStats().getQueueOperationStats().getPeriodEnd();
            stat.periodStart = memberQStat.getLocalQueueStats().getQueueOperationStats().getPeriodStart();
            stat.memberName = memberQStat.getMember().getInetSocketAddress().getHostName() + ":"
                    + memberQStat.getMember().getInetSocketAddress().getPort();
            long periodInSec = (stat.periodEnd - stat.periodStart) / 1000;
            LocalQueueOperationStats localQueueOperationStats = memberQStat.getLocalQueueStats().getQueueOperationStats();
            stat.numberOfEmptyPollsInSec = localQueueOperationStats.getNumberOfEmptyPolls() / periodInSec;
            stat.numberOfOffersInSec = localQueueOperationStats.getNumberOfOffers() / periodInSec;
            stat.numberOfPollsInSec = localQueueOperationStats.getNumberOfPolls() / periodInSec;
            stat.numberOfRejectedOffersInSec = localQueueOperationStats.getNumberOfRejectedOffers() / periodInSec;
            listOfStats.add(stat);
        }
        QueueStatistics event = new QueueStatistics(this.clusterId);
        event.setName(name);
        event.setList(listOfStats);
        storeEvent(event);
        return event;
    }

    public ChangeEventType getChangeEventType() {
        return ChangeEventType.QUEUE_STATISTICS;
    }

    public int getClusterId() {
        return clusterId;
    }
}
