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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.Member;

import java.io.Serializable;
import java.util.concurrent.Callable;

public class DistributedQueueStatsCallable implements Callable<DistributedQueueStatsCallable.MemberQueueStats>, Serializable, HazelcastInstanceAware {
    private String qName;
    private transient HazelcastInstance hzInstance;

    public DistributedQueueStatsCallable(String qName) {
        this.qName = qName;
    }

    public MemberQueueStats call() throws Exception {
        IQueue queue = hzInstance.getQueue(qName);
        LocalQueueStats localMapStats = queue.getLocalQueueStats();
        Member member = hzInstance.getCluster().getLocalMember();
        return new MemberQueueStats(member, localMapStats);
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        hzInstance = hazelcastInstance;
    }

    public static class MemberQueueStats implements Serializable {
        Member member;
        LocalQueueStats localQueueStats;

        public MemberQueueStats(Member member, LocalQueueStats localQueueStats) {
            this.member = member;
            this.localQueueStats = localQueueStats;
        }

        public Member getMember() {
            return member;
        }

        public void setMember(Member member) {
            this.member = member;
        }

        public LocalQueueStats getLocalQueueStats() {
            return localQueueStats;
        }

        public void setLocalQueueStats(LocalQueueStats localQueueStats) {
            this.localQueueStats = localQueueStats;
        }
    }
}