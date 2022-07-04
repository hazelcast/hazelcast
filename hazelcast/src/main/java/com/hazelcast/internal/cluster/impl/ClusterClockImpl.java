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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.internal.cluster.ClusterClock;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.logging.ILogger;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLUSTER_METRIC_CLUSTER_CLOCK_CLUSTER_START_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLUSTER_METRIC_CLUSTER_CLOCK_CLUSTER_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLUSTER_METRIC_CLUSTER_CLOCK_CLUSTER_TIME_DIFF;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLUSTER_METRIC_CLUSTER_CLOCK_CLUSTER_UP_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLUSTER_METRIC_CLUSTER_CLOCK_LOCAL_CLOCK_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLUSTER_METRIC_CLUSTER_CLOCK_MAX_CLUSTER_TIME_DIFF;
import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.metrics.ProbeUnit.MS;
import static java.lang.Math.abs;


public class ClusterClockImpl implements ClusterClock {

    private final ILogger logger;

    private volatile long clusterTimeDiff;
    private volatile long clusterStartTime = Long.MIN_VALUE;
    @Probe(name = CLUSTER_METRIC_CLUSTER_CLOCK_MAX_CLUSTER_TIME_DIFF, level = MANDATORY, unit = MS)
    private volatile long maxClusterTimeDiff;

    public ClusterClockImpl(ILogger logger) {
        this.logger = logger;
    }

    @Probe(name = CLUSTER_METRIC_CLUSTER_CLOCK_CLUSTER_TIME, unit = MS, level = DEBUG)
    @Override
    public long getClusterTime() {
        return Clock.currentTimeMillis() + clusterTimeDiff;
    }

    /**
     * Calculate and set the cluster clock diff.
     *
     * @param masterTime the cluster clock timestamp sent by the master node
     */
    public void setMasterTime(long masterTime) {
        long diff = masterTime - Clock.currentTimeMillis();
        setClusterTimeDiff(diff);
    }

    /** Set the cluster time diff and records the maximum observed cluster time diff */
    void setClusterTimeDiff(long diff) {
        if (logger.isFineEnabled()) {
            logger.fine("Setting cluster time diff to " + diff + "ms.");
        }

        if (abs(diff) > abs(maxClusterTimeDiff)) {
            maxClusterTimeDiff = diff;
        }

        this.clusterTimeDiff = diff;
    }

    @Probe(name = CLUSTER_METRIC_CLUSTER_CLOCK_CLUSTER_TIME_DIFF, level = MANDATORY, unit = MS)
    long getClusterTimeDiff() {
        return clusterTimeDiff;
    }

    @Probe(name = CLUSTER_METRIC_CLUSTER_CLOCK_CLUSTER_UP_TIME, unit = MS)
    @Override
    public long getClusterUpTime() {
        return Clock.currentTimeMillis() - clusterStartTime;
    }

    public void setClusterStartTime(long startTime) {
        if (this.clusterStartTime == Long.MIN_VALUE) {
            this.clusterStartTime = startTime;
        }
    }

    @Probe(name = CLUSTER_METRIC_CLUSTER_CLOCK_LOCAL_CLOCK_TIME, unit = MS, level = DEBUG)
    private long getLocalClockTime() {
        return Clock.currentTimeMillis();
    }

    @Probe(name = CLUSTER_METRIC_CLUSTER_CLOCK_CLUSTER_START_TIME, unit = MS, level = DEBUG)
    public long getClusterStartTime() {
        return clusterStartTime;
    }

}
