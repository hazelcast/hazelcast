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

package com.hazelcast.monitor;

import com.hazelcast.core.*;

import java.io.Serializable;
import java.util.concurrent.Callable;

public class DistributedMapStatsCallable implements Callable<DistributedMapStatsCallable.MemberMapStat>, Serializable, HazelcastInstanceAware {
    private String mapName;
    private HazelcastInstance hzInstance;

    public DistributedMapStatsCallable(String mapName) {
        this.mapName = mapName;
    }

    public MemberMapStat call() throws Exception {
        IMap map = hzInstance.getMap(mapName);
        LocalMapStats localMapStats = map.getLocalMapStats();
        Member member = hzInstance.getCluster().getLocalMember();
        return new MemberMapStat(member, localMapStats);
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        hzInstance = hazelcastInstance;
    }

    public static class MemberMapStat implements Serializable{
        Member member;
        LocalMapStats localMapStats;


        public MemberMapStat(Member member, LocalMapStats localMapStats) {
            this.member = member;
            this.localMapStats = localMapStats;
        }

        public Member getMember() {
            return member;
        }

        public void setMember(Member member) {
            this.member = member;
        }

        public LocalMapStats getLocalMapStats() {
            return localMapStats;
        }

        public void setLocalMapStats(LocalMapStats localMapStats) {
            this.localMapStats = localMapStats;
        }
    }
}