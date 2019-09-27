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

package com.hazelcast.gcp;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.internal.util.ExceptionUtil;

import java.util.concurrent.Callable;

/**
 * Static utility class to retry operations.
 */
final class RetryUtils {
    static final long INITIAL_BACKOFF_MS = 1500L;
    static final double BACKOFF_MULTIPLIER = 1.5;
    private static final long MAX_BACKOFF_MS = 5 * 60 * 1000L;

    private static final ILogger LOGGER = Logger.getLogger(RetryUtils.class);

    private static final long MS_IN_SECOND = 1000L;

    private RetryUtils() {
    }

    /**
     * Calls {@code callable.call()} until it does not throw an exception (but no more than {@code retries} times).
     * <p>
     * Note that {@code callable} should be an idempotent operation.
     * <p>
     * If {@code callable} throws an unchecked exception, it is wrapped into {@link HazelcastException}.
     */
    static <T> T retry(Callable<T> callable, int retries) {
        int retryCount = 0;
        while (true) {
            try {
                return callable.call();
            } catch (Exception e) {
                retryCount++;
                if (retryCount > retries) {
                    throw ExceptionUtil.rethrow(e);
                }
                long waitIntervalMs = backoffIntervalForRetry(retryCount);
                LOGGER.warning(String.format("Couldn't connect to the service, [%s] retrying in %s seconds...", retryCount,
                        waitIntervalMs / MS_IN_SECOND));
                sleep(waitIntervalMs);
            }
        }
    }

    private static long backoffIntervalForRetry(int retryCount) {
        long result = INITIAL_BACKOFF_MS;
        for (int i = 1; i < retryCount; i++) {
            result *= BACKOFF_MULTIPLIER;
            if (result > MAX_BACKOFF_MS) {
                return MAX_BACKOFF_MS;
            }
        }
        return result;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new HazelcastException(e);
        }
    }
}
