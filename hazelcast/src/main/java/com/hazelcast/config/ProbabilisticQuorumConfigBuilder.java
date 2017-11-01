/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.quorum.impl.ProbabilisticQuorumFunction;

import static com.hazelcast.internal.cluster.fd.PhiAccrualClusterFailureDetector
        .HEARTBEAT_PHI_FAILURE_DETECTOR_MIN_STD_DEV_MILLIS;
import static com.hazelcast.internal.cluster.fd.PhiAccrualClusterFailureDetector.HEARTBEAT_PHI_FAILURE_DETECTOR_SAMPLE_SIZE;
import static com.hazelcast.internal.cluster.fd.PhiAccrualClusterFailureDetector.HEARTBEAT_PHI_FAILURE_DETECTOR_THRESHOLD;
import static com.hazelcast.spi.properties.GroupProperty.HEARTBEAT_INTERVAL_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MAX_NO_HEARTBEAT_SECONDS;
import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Builder for a {@link QuorumConfig} configured with {@link com.hazelcast.quorum.impl.ProbabilisticQuorumFunction} that
 * considers members present in the context of a quorum using a probabilistic, adaptive quorum function.
 */
public class ProbabilisticQuorumConfigBuilder extends QuorumConfigBuilder {

    /**
     * Default threshold for suspicion (φ) level.
     */
    public static final double DEFAULT_PHI_THRESHOLD = parseDouble(HEARTBEAT_PHI_FAILURE_DETECTOR_THRESHOLD.getDefaultValue());
    /**
     * Default number of samples to use for calculation of mean and standard deviation of inter-arrival times.
     */
    public static final int DEFAULT_SAMPLE_SIZE = parseInt(HEARTBEAT_PHI_FAILURE_DETECTOR_SAMPLE_SIZE.getDefaultValue());
    /**
     * Default minimum standard deviation (in milliseconds) to use for the normal distribution used when
     * calculating phi.
     */
    public static final long DEFAULT_MIN_STD_DEVIATION =
                parseLong(HEARTBEAT_PHI_FAILURE_DETECTOR_MIN_STD_DEV_MILLIS.getDefaultValue());
    /**
     * Default duration in milliseconds corresponding to number of potentially lost/delayed heartbeats that will
     * be accepted before considering it to be an anomaly.
     */
    public static final long DEFAULT_HEARTBEAT_PAUSE_MILLIS =
                SECONDS.toMillis(parseInt(MAX_NO_HEARTBEAT_SECONDS.getDefaultValue()));
    /**
     * Default value of heartbeat interval (in milliseconds).
     */
    public static final long DEFAULT_HEARTBEAT_INTERVAL_MILLIS =
                SECONDS.toMillis(parseInt(HEARTBEAT_INTERVAL_SECONDS.getDefaultValue()));

    private final String name;
    private double phiThreshold = DEFAULT_PHI_THRESHOLD;
    private int maxSampleSize = DEFAULT_SAMPLE_SIZE;
    private long minStdDeviationMillis = DEFAULT_MIN_STD_DEVIATION;
    private long acceptableHeartbeatPauseMillis = DEFAULT_HEARTBEAT_PAUSE_MILLIS;
    private long heartbeatIntervalMillis = DEFAULT_HEARTBEAT_INTERVAL_MILLIS;

    ProbabilisticQuorumConfigBuilder(String name, int size) {
        this.name = name;
        this.size = size;
    }

    /**
     * Threshold for suspicion (φ) level. A low threshold is prone to generate many wrong suspicions but ensures
     * a quick detection in the event of a real crash. Conversely, a high threshold generates fewer mistakes but needs
     * more time to detect actual crashes.
     */
    public ProbabilisticQuorumConfigBuilder withSuspicionThreshold(double suspicionThreshold) {
        this.phiThreshold = suspicionThreshold;
        return this;
    }

    /**
     * Number of samples to use for calculation of mean and standard deviation of inter-arrival times.
     */
    public ProbabilisticQuorumConfigBuilder withMaxSampleSize(int maxSampleSize) {
        this.maxSampleSize = maxSampleSize;
        return this;
    }

    /**
     * Minimum standard deviation (in milliseconds) to use for the normal distribution used when calculating phi.
     * Too low standard deviation might result in too much sensitivity for sudden, but normal, deviations in heartbeat
     * inter arrival times.
     */
    public ProbabilisticQuorumConfigBuilder withMinStdDeviationMillis(long minStdDeviationMillis) {
        this.minStdDeviationMillis = minStdDeviationMillis;
        return this;
    }

    /**
     * Duration in milliseconds corresponding to number of potentially lost/delayed heartbeats that will be accepted
     * before considering it to be an anomaly. This margin is important to be able to survive sudden, occasional,
     * pauses in heartbeat arrivals, due to for example garbage collection or network drops.
     */
    public ProbabilisticQuorumConfigBuilder withAcceptableHeartbeatPauseMillis(long acceptableHeartbeatPauseMillis) {
        this.acceptableHeartbeatPauseMillis = acceptableHeartbeatPauseMillis;
        return this;
    }

    /**
     * Bootstrap the stats with heartbeats that corresponds to this duration in milliseconds, with a rather high
     * standard deviation (since environment is unknown in the beginning).
     */
    public ProbabilisticQuorumConfigBuilder withHeartbeatIntervalMillis(long heartbeatIntervalMillis) {
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
        return this;
    }

    public QuorumConfig build() {
        ProbabilisticQuorumFunction quorumFunction = new ProbabilisticQuorumFunction(size, heartbeatIntervalMillis,
                acceptableHeartbeatPauseMillis, maxSampleSize, minStdDeviationMillis, phiThreshold);

        QuorumConfig config = new QuorumConfig();
        config.setName(name);
        config.setEnabled(enabled);
        config.setSize(size);
        config.setQuorumFunctionImplementation(quorumFunction);
        return config;
    }
}
