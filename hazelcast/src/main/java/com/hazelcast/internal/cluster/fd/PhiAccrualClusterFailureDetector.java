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
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.internal.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutIfAbsent;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Cluster failure detector based on 'The Phi Accrual Failure Detector' by Hayashibara et al.
 * For more info see {@link PhiAccrualFailureDetector}
 */
public class PhiAccrualClusterFailureDetector implements ClusterFailureDetector {

    /**
     * Threshold for suspicion level. After calculated phi exceeds this threshold, a member is considered as dead.
     * <p>
     * <ul>
     * <li>
     * A low threshold allows to detect member crashes/failures faster but can generate more mistakes
     * and cause wrong member suspicions.
     * </li>
     * <li>
     * A high threshold generates fewer mistakes but is slower detect actual crashes/failures.
     * </li>
     * </ul>
     */
    public static final HazelcastProperty HEARTBEAT_PHI_FAILURE_DETECTOR_THRESHOLD
            = new HazelcastProperty("hazelcast.heartbeat.phiaccrual.failuredetector.threshold", 10);

    /**
     * Number of samples to use for calculations.
     */
    public static final HazelcastProperty HEARTBEAT_PHI_FAILURE_DETECTOR_SAMPLE_SIZE
            = new HazelcastProperty("hazelcast.heartbeat.phiaccrual.failuredetector.sample.size", 200);

    /**
     * Minimum standard deviation to use for the normal distribution used when calculating phi.
     * Too low standard deviation might result in too much sensitivity.
     */
    public static final HazelcastProperty HEARTBEAT_PHI_FAILURE_DETECTOR_MIN_STD_DEV_MILLIS
            = new HazelcastProperty("hazelcast.heartbeat.phiaccrual.failuredetector.min.std.dev.millis", 100, MILLISECONDS);


    private final double phiThreshold;
    private final ConcurrentMap<Member, FailureDetector> failureDetectors = new ConcurrentHashMap<>();
    private final ConstructorFunction<Member, FailureDetector> failureDetectorConstructor;

    public PhiAccrualClusterFailureDetector(long maxNoHeartbeatMillis, long heartbeatIntervalMillis, HazelcastProperties props) {
        this(maxNoHeartbeatMillis, heartbeatIntervalMillis, props.getDouble(HEARTBEAT_PHI_FAILURE_DETECTOR_THRESHOLD),
                props.getInteger(HEARTBEAT_PHI_FAILURE_DETECTOR_SAMPLE_SIZE),
                props.getMillis(HEARTBEAT_PHI_FAILURE_DETECTOR_MIN_STD_DEV_MILLIS));
    }

    public PhiAccrualClusterFailureDetector(long maxNoHeartbeatMillis, long heartbeatIntervalMillis, double phiThreshold,
                                            int maxSampleSize, long minStdDeviationMillis) {
        this.phiThreshold = phiThreshold;
        this.failureDetectorConstructor = arg -> new PhiAccrualFailureDetector(phiThreshold, maxSampleSize, minStdDeviationMillis,
                maxNoHeartbeatMillis, heartbeatIntervalMillis);
    }

    @Override
    public void heartbeat(Member member, long timestamp) {
        FailureDetector fd = getOrPutIfAbsent(failureDetectors, member, failureDetectorConstructor);
        fd.heartbeat(timestamp);
    }

    @Override
    public boolean isAlive(Member member, long timestamp) {
        FailureDetector fd = failureDetectors.get(member);
        return fd != null && fd.isAlive(timestamp);
    }

    @Override
    public long lastHeartbeat(Member member) {
        FailureDetector fd = failureDetectors.get(member);
        return fd != null ? fd.lastHeartbeat() : 0L;
    }

    @Override
    public double suspicionLevel(Member member, long timestamp) {
        FailureDetector fd = failureDetectors.get(member);
        return fd != null ? fd.suspicionLevel(timestamp) : phiThreshold;
    }

    @Override
    public void remove(Member member) {
        failureDetectors.remove(member);
    }

    @Override
    public void reset() {
        failureDetectors.clear();
    }
}
