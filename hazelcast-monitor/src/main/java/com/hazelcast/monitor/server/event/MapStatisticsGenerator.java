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
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiTask;
import com.hazelcast.monitor.LocalMapOperationStats;
import com.hazelcast.monitor.DistributedMapStatsCallable;
import com.hazelcast.monitor.client.event.ChangeEvent;
import com.hazelcast.monitor.client.event.ChangeEventType;
import com.hazelcast.monitor.client.event.MapStatistics;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

public class MapStatisticsGenerator extends InstanceStatisticsGenerator {

    public MapStatisticsGenerator(HazelcastClient client, String instanceName, int clusterId) {
        super(instanceName, client, clusterId);
    }



    public synchronized ChangeEvent generateEvent() {
        ExecutorService esService = client.getExecutorService();
        Set<Member> members = client.getCluster().getMembers();
        final List<Member> lsMembers = new ArrayList<Member>(members);
        MultiTask<DistributedMapStatsCallable.MemberMapStat> task =
                new MultiTask<DistributedMapStatsCallable.MemberMapStat>(new DistributedMapStatsCallable(name), members);
        esService.execute(task);
        Collection<DistributedMapStatsCallable.MemberMapStat> mapStats;
        try {
            mapStats = task.get();
        } catch (InterruptedException e) {
            return null;
        } catch (ExecutionException e) {
            return null;
        }
        if (mapStats == null) {
            return null;
        }
        if (members.size() != mapStats.size()) {
            return null;
        }

        List<DistributedMapStatsCallable.MemberMapStat> lsMapStats = new ArrayList(mapStats);
        Collections.sort(lsMapStats, new Comparator<DistributedMapStatsCallable.MemberMapStat>() {
            public int compare(DistributedMapStatsCallable.MemberMapStat o1, DistributedMapStatsCallable.MemberMapStat o2) {
                int i1 = lsMembers.indexOf(o1.getMember());
                int i2 = lsMembers.indexOf(o2.getMember());
                return i1 - i2;
            }
        });

        List<MapStatistics.LocalMapStatistics> listOfStats = new ArrayList<MapStatistics.LocalMapStatistics>();
        for (DistributedMapStatsCallable.MemberMapStat memberMapStat : lsMapStats) {
            MapStatistics.LocalMapStatistics stat = new MapStatistics.LocalMapStatistics();
            stat.backupEntryCount = memberMapStat.getLocalMapStats().getBackupEntryCount();
            stat.backupEntryMemoryCost = memberMapStat.getLocalMapStats().getBackupEntryMemoryCost();
            stat.creationTime = memberMapStat.getLocalMapStats().getCreationTime();
            stat.hits = memberMapStat.getLocalMapStats().getHits();
            stat.lastAccessTime = memberMapStat.getLocalMapStats().getLastAccessTime();
            stat.lastUpdateTime = memberMapStat.getLocalMapStats().getLastUpdateTime();
            stat.lockedEntryCount = memberMapStat.getLocalMapStats().getLockedEntryCount();
            stat.lockWaitCount = memberMapStat.getLocalMapStats().getLockWaitCount();
            stat.markedAsRemovedEntryCount = memberMapStat.getLocalMapStats().getMarkedAsRemovedEntryCount();
            stat.markedAsRemovedMemoryCost = memberMapStat.getLocalMapStats().getMarkedAsRemovedMemoryCost();
            stat.ownedEntryCount = memberMapStat.getLocalMapStats().getOwnedEntryCount();
            stat.ownedEntryMemoryCost = memberMapStat.getLocalMapStats().getOwnedEntryMemoryCost();
            stat.lastEvictionTime = memberMapStat.getLocalMapStats().getLastEvictionTime();
            stat.memberName = memberMapStat.getMember().getInetSocketAddress().getHostName() + ":"
                    + memberMapStat.getMember().getInetSocketAddress().getPort();
            LocalMapOperationStats mapOpStats = memberMapStat.getLocalMapStats().getOperationStats();
            stat.periodStart = mapOpStats.getPeriodStart();
            stat.periodEnd = mapOpStats.getPeriodEnd();
            long periodInSec = (stat.periodEnd - stat.periodStart) / 1000;
            if (periodInSec != 0) {
                stat.numberOfPutsInSec = mapOpStats.getNumberOfPuts() / periodInSec;
                stat.numberOfGetsInSec = mapOpStats.getNumberOfGets() / periodInSec;
                stat.numberOfRemovesInSec = mapOpStats.getNumberOfRemoves() / periodInSec;
                stat.numberOfOthersInSec = mapOpStats.getNumberOfOtherOperations() / periodInSec;
                stat.numberOfEventsInSec = mapOpStats.getNumberOfEvents() / periodInSec;
            }
            listOfStats.add(stat);
        }
        MapStatistics event = new MapStatistics(clusterId);
        event.setName(this.name);
        event.setListOfLocalStats(listOfStats);
        storeEvent(event);
        return event;
    }

    public ChangeEventType getChangeEventType() {
        return ChangeEventType.MAP_STATISTICS;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MapStatisticsGenerator that = (MapStatisticsGenerator) o;
        if (clusterId != that.clusterId) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = clusterId;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }
}
