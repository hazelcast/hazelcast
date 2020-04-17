/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.connection.tcp;

import com.hazelcast.internal.util.Clock;
import com.hazelcast.logging.ILogger;

import java.util.Random;

public class WaitStrategy {

    private final int initialBackoffMillis;
    private final int maxBackoffMillis;
    private final double multiplier;
    private final double jitter;
    private final Random random = new Random();
    private final ILogger logger;
    private int attempt;
    private int currentBackoffMillis;
    private long clusterConnectTimeoutMillis;
    private long clusterConnectAttemptBegin;

    WaitStrategy(int initialBackoffMillis, int maxBackoffMillis,
                 double multiplier, long clusterConnectTimeoutMillis, double jitter, ILogger logger) {
        this.initialBackoffMillis = initialBackoffMillis;
        this.maxBackoffMillis = maxBackoffMillis;
        this.multiplier = multiplier;
        this.clusterConnectTimeoutMillis = clusterConnectTimeoutMillis;
        this.jitter = jitter;
        this.logger = logger;
    }

    public void reset() {
        attempt = 0;
        clusterConnectAttemptBegin = Clock.currentTimeMillis();
        currentBackoffMillis = Math.min(maxBackoffMillis, initialBackoffMillis);
    }

    public boolean sleep() {
        attempt++;
        long currentTimeMillis = Clock.currentTimeMillis();
        long timePassed = currentTimeMillis - clusterConnectAttemptBegin;
        if (timePassed > clusterConnectTimeoutMillis) {
            logger.warning(String.format("Unable to get live cluster connection, cluster connect timeout (%d millis) is "
                    + "reached. Attempt %d.", clusterConnectTimeoutMillis, attempt));
            return false;
        }

        //random_between
        // Random(-jitter * current_backoff, jitter * current_backoff)
        long actualSleepTime = (long) (currentBackoffMillis - (currentBackoffMillis * jitter)
                + (currentBackoffMillis * jitter * random.nextDouble()));

        actualSleepTime = Math.min(actualSleepTime, clusterConnectTimeoutMillis - timePassed);

        logger.warning(String.format("Unable to get live cluster connection, retry in %d ms, attempt: %d "
                + ", cluster connect timeout: %d seconds "
                + ", max backoff millis: %d", actualSleepTime, attempt, clusterConnectTimeoutMillis, maxBackoffMillis));

        try {
            Thread.sleep(actualSleepTime);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }

        currentBackoffMillis = (int) Math.min(currentBackoffMillis * multiplier, maxBackoffMillis);
        return true;
    }
}
