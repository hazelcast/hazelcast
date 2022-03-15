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

package com.hazelcast.internal.cluster.fd;

import com.hazelcast.cluster.Member;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Deadline based failure detector. This failure detector uses an absolute timeout
 * for missing/lost heartbeats. After timeout member is considered as dead/unavailable.
 */
public class DeadlineClusterFailureDetector implements ClusterFailureDetector {

    private final long maxNoHeartbeatMillis;
    private final ConcurrentMap<Member, Long> heartbeatTimes = new ConcurrentHashMap<>();

    public DeadlineClusterFailureDetector(long maxNoHeartbeatMillis) {
        this.maxNoHeartbeatMillis = maxNoHeartbeatMillis;
    }

    @Override
    public void heartbeat(Member member, long timestamp) {
        heartbeatTimes.put(member, timestamp);
    }

    @Override
    public boolean isAlive(Member member, long timestamp) {
        long hb = lastHeartbeat(member);
        return hb + maxNoHeartbeatMillis > timestamp;
    }

    @Override
    public long lastHeartbeat(Member member) {
        Long hb = heartbeatTimes.get(member);
        return hb != null ? hb : 0L;
    }

    @Override
    public double suspicionLevel(Member member, long timestamp) {
        return isAlive(member, timestamp) ? 0 : 1;
    }

    @Override
    public void remove(Member member) {
        heartbeatTimes.remove(member);
    }

    @Override
    public void reset() {
        heartbeatTimes.clear();
    }
}
