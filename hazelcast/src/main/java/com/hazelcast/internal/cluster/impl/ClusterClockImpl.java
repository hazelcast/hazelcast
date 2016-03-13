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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.internal.cluster.ClusterClock;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.Clock;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;


public class ClusterClockImpl implements ClusterClock {

    private final ILogger logger;

    private volatile long clusterTimeDiff;
    private volatile long clusterStartTime = Long.MIN_VALUE;

    public ClusterClockImpl(ILogger logger) {
        this.logger = logger;
    }

    @Probe(name = "clusterTime")
    @Override
    public long getClusterTime() {
        return Clock.currentTimeMillis() + clusterTimeDiff;
    }

    public void setMasterTime(long masterTime) {
        long diff = masterTime - Clock.currentTimeMillis();
        setClusterTimeDiff(diff);
    }

    void setClusterTimeDiff(long diff) {
        if (logger.isFinestEnabled()) {
            logger.finest("Setting cluster time diff to " + diff + "ms.");
        }
        this.clusterTimeDiff = diff;
    }

    @Probe(name = "clusterTimeDiff", level = MANDATORY)
    long getClusterTimeDiff() {
        return clusterTimeDiff;
    }

    @Probe(name = "clusterUpTime")
    @Override
    public long getClusterUpTime() {
        return Clock.currentTimeMillis() - clusterStartTime;
    }

    public void setClusterStartTime(long startTime) {
        if (this.clusterStartTime == Long.MIN_VALUE) {
            this.clusterStartTime = startTime;
        }
    }

    @Probe(name = "localClockTime")
    private long getLocalClockTime() {
        return Clock.currentTimeMillis();
    }

    @Probe(name = "clusterStartTime")
    public long getClusterStartTime() {
        return clusterStartTime;
    }

}
