/*
 * Copyright (C) 2009-2017 Lightbend Inc. <http://www.lightbend.com>
 * Copyright (c) 2017-2021, Hazelcast, Inc. All Rights Reserved.
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

import java.util.LinkedList;

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.util.Preconditions.checkPositive;

/**
 * Port of Akka's PhiAccrualFailureDetector.scala
 * <p>
 * Implementation of 'The Phi Accrual Failure Detector' by Hayashibara et al. as defined in their paper.
 * <p>
 * The suspicion level of failure is given by a value called φ (phi).
 * The basic idea of the φ failure detector is to express the value of φ on a scale that
 * is dynamically adjusted to reflect current network conditions. A configurable
 * threshold is used to decide if <code>φ</code> is considered to be a failure.
 * <p>
 * The value of <code>φ</code> is calculated as:
 * <p>
 * <pre>
 * φ = -log10(1 - F(timeSinceLastHeartbeat)
 * </pre>
 * where F is the cumulative distribution function of a normal distribution with mean
 * and standard deviation estimated from historical heartbeat inter-arrival times.
 */
public class PhiAccrualFailureDetector implements FailureDetector {

    static final long NO_HEARTBEAT_TIMESTAMP = -1;

    private final double threshold;
    private final double minStdDeviationMillis;
    private final long acceptableHeartbeatPauseMillis;

    private final HeartbeatHistory heartbeatHistory;
    private volatile long lastHeartbeatMillis = NO_HEARTBEAT_TIMESTAMP;

    /**
     * @param threshold                      A low threshold is prone to generate many wrong suspicions but ensures
     *                                       a quick detection in the event of a real crash. Conversely, a high threshold
     *                                       generates fewer mistakes but needs more time to detect actual crashes
     * @param maxSampleSize                  Number of samples to use for calculation of mean and standard deviation of
     *                                       inter-arrival times.
     * @param minStdDeviationMillis          Minimum standard deviation to use for the normal distribution used when
     *                                       calculating phi. Too low standard deviation might result in too much sensitivity
     *                                       for sudden, but normal, deviations in heartbeat inter arrival times.
     * @param acceptableHeartbeatPauseMillis Duration corresponding to number of potentially lost/delayed
     *                                       heartbeats that will be accepted before considering it to be an anomaly.
     *                                       This margin is important to be able to survive sudden, occasional, pauses
     *                                       in heartbeat arrivals, due to for example garbage collect or network drop.
     * @param firstHeartbeatEstimateMillis   Bootstrap the stats with heartbeats that corresponds to this duration,
     *                                       with a with rather high standard deviation (since environment is unknown
     *                                       in the beginning)
     */
    public PhiAccrualFailureDetector(double threshold, int maxSampleSize, double minStdDeviationMillis,
                                     long acceptableHeartbeatPauseMillis, long firstHeartbeatEstimateMillis) {

        this.threshold = checkPositive(threshold, "Threshold must be positive: " + threshold);
        this.minStdDeviationMillis = checkPositive(minStdDeviationMillis, "Minimum standard deviation must be positive: "
                + minStdDeviationMillis);

        this.acceptableHeartbeatPauseMillis = checkNotNegative(acceptableHeartbeatPauseMillis,
                "Acceptable heartbeat pause millis must be >= 0: " + acceptableHeartbeatPauseMillis);

        checkPositive("firstHeartbeatEstimateMillis", firstHeartbeatEstimateMillis);

        heartbeatHistory = new HeartbeatHistory(maxSampleSize);
        firstHeartbeat(firstHeartbeatEstimateMillis);
    }

    // guess statistics for first heartbeat,
    // important so that connections with only one heartbeat becomes unavailable
    // bootstrap with 2 entries with rather high standard deviation
    @SuppressWarnings("checkstyle:magicnumber")
    private void firstHeartbeat(long firstHeartbeatEstimateMillis) {
        long stdDeviationMillis = firstHeartbeatEstimateMillis / 4;
        heartbeatHistory.add(firstHeartbeatEstimateMillis - stdDeviationMillis);
        heartbeatHistory.add(firstHeartbeatEstimateMillis + stdDeviationMillis);
    }

    private double ensureValidStdDeviation(double stdDeviationMillis) {
        return Math.max(stdDeviationMillis, minStdDeviationMillis);
    }

    /**
     * The suspicion level of the accrual failure detector.
     *
     * If a connection does not have any records in failure detector then it is
     * considered healthy.
     */
    private double phi(long timestampMillis) {
        long timeDiffMillis;
        double meanMillis;
        double stdDeviationMillis;

        synchronized (heartbeatHistory) {
            long lastTimestampMillis = lastHeartbeatMillis;
            if (lastTimestampMillis == NO_HEARTBEAT_TIMESTAMP) {
                return 0.0;
            }

            timeDiffMillis = timestampMillis - lastTimestampMillis;
            meanMillis = heartbeatHistory.mean();
            stdDeviationMillis = ensureValidStdDeviation(heartbeatHistory.stdDeviation());
        }

        return phi(timeDiffMillis, meanMillis + acceptableHeartbeatPauseMillis, stdDeviationMillis);
    }

    /**
     * Calculation of phi, derived from the Cumulative distribution function for
     * N(mean, stdDeviation) normal distribution, given by
     * 1.0 / (1.0 + math.exp(-y * (1.5976 + 0.070566 * y * y)))
     * where y = (x - mean) / standard_deviation
     * This is an approximation defined in β Mathematics Handbook (Logistic approximation).
     * Error is 0.00014 at +- 3.16
     * The calculated value is equivalent to -log10(1 - CDF(y))
     */
    @SuppressWarnings("checkstyle:magicnumber")
    private static double phi(long timeDiffMillis, double meanMillis, double stdDeviationMillis) {
        double y = (timeDiffMillis - meanMillis) / stdDeviationMillis;
        double e = Math.exp(-y * (1.5976 + 0.070566 * y * y));
        if (timeDiffMillis > meanMillis) {
            return -Math.log10(e / (1.0 + e));
        } else {
            return -Math.log10(1.0 - 1.0 / (1.0 + e));
        }
    }

    @Override
    public boolean isAlive(long timestampMillis) {
        double phi = phi(timestampMillis);
        return phi < threshold;
    }

    @Override
    public void heartbeat(long timestampMillis) {
        synchronized (heartbeatHistory) {
            long lastTimestampMillis = getAndSetLastHeartbeat(timestampMillis);
            if (lastTimestampMillis == NO_HEARTBEAT_TIMESTAMP) {
                return;
            }

            if (isAlive(timestampMillis)) {
                heartbeatHistory.add(timestampMillis - lastTimestampMillis);
            }
        }
    }

    private long getAndSetLastHeartbeat(long timestampMillis) {
        long lastTimestampMillis = lastHeartbeatMillis;
        lastHeartbeatMillis = timestampMillis;
        return lastTimestampMillis;
    }

    @Override
    public long lastHeartbeat() {
        return lastHeartbeatMillis;
    }

    @Override
    public double suspicionLevel(long timestamp) {
        return phi(timestamp);
    }

    /**
     * Holds the heartbeat statistics for a specific member.
     * It is capped by the number of samples specified in `maxSampleSize`.
     *
     * The stats (mean, variance, stdDeviation) are not defined for
     * for empty HeartbeatHistory, i.e. throws ArithmeticException.
     */
    private static class HeartbeatHistory {
        private final int maxSampleSize;
        private final LinkedList<Long> intervals = new LinkedList<>();
        private long intervalSum;
        private long squaredIntervalSum;

        HeartbeatHistory(int maxSampleSize) {
            if (maxSampleSize < 1) {
                throw new IllegalArgumentException("Sample size must be >= 1 : " + maxSampleSize);
            }
            this.maxSampleSize = maxSampleSize;
        }

        double mean() {
            return (double) intervalSum / intervals.size();
        }

        double variance() {
            double mean = mean();
            return ((double) squaredIntervalSum / intervals.size()) - (mean * mean);
        }

        double stdDeviation() {
            return Math.sqrt(variance());
        }

        void add(long interval) {
            if (intervals.size() >= maxSampleSize) {
                dropOldest();
            }
            intervals.add(interval);
            intervalSum += interval;
            squaredIntervalSum += pow2(interval);
        }

        private void dropOldest() {
            long dropped = intervals.pollFirst();
            intervalSum -= dropped;
            squaredIntervalSum -= pow2(dropped);
        }

        private static long pow2(long x) {
            return x * x;
        }
    }
}
