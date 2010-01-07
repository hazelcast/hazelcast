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
import com.hazelcast.monitor.DistributedMapStatsCallable;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.client.event.ChangeEvent;
import com.hazelcast.monitor.client.event.ChangeEventType;
import com.hazelcast.monitor.client.event.MapStatistics;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

public class MapStatisticsGenerator implements ChangeEventGenerator {
    private int clusterId;
    private IMap map;
    private List<MapStatistics> list = new ArrayList();
    private HazelcastClient client;
    private String mapName;


    public MapStatisticsGenerator(HazelcastClient client, String instanceName, int clusterId) {
        this.clusterId = clusterId;
        this.client = client;
        this.mapName = instanceName;
        map = client.getMap(mapName);
    }

    public List<MapStatistics> getPastMapStatistics() {
        return list;
    }

    public synchronized ChangeEvent generateEvent() {
        ExecutorService esService = client.getExecutorService();
        Set<Member> members = client.getCluster().getMembers();
        System.out.println("Members: "+ members.size());
        MultiTask<DistributedMapStatsCallable.MemberMapStat> task =
                new MultiTask<DistributedMapStatsCallable.MemberMapStat>(new DistributedMapStatsCallable(mapName),members);

        esService.execute(task);
        Collection<DistributedMapStatsCallable.MemberMapStat> mapStats = null;
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
        List<MapStatistics.LocalMapStatistics> listOfStats = new ArrayList<MapStatistics.LocalMapStatistics>();
        System.out.println("Size of list returned from exs : "+ mapStats.size());        
        for (DistributedMapStatsCallable.MemberMapStat memberMapStat:mapStats) {
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
                    +memberMapStat.getMember().getInetSocketAddress().getPort();

            listOfStats.add(stat);

        }
        System.out.println("Returning List of Stats: "+ listOfStats.size());

        MapStatistics event = new MapStatistics(clusterId);
        event.setSize(map.size());
        event.setListOfLocalStats(listOfStats);
        if(!list.isEmpty() && list.get(list.size()-1).equals(event)){
            list.remove(list.size()-1);
        }
        list.add(event);
        while (list.size() > 100) {
            list.remove(0);
        }
        return event;
    }

    public ChangeEventType getChangeEventType() {
        return ChangeEventType.MAP_STATISTICS;
    }

    public int getClusterId() {
        return clusterId;
    }

    public String getName() {
        return map.getName();
    }
}
