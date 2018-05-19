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

package com.hazelcast.aws.utility;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Callable;

/**
 * Static utility class to retry operations related to connecting to AWS Services.
 */
public final class RetryUtils {
    private static final ILogger LOGGER = Logger.getLogger(RetryUtils.class);

    private RetryUtils() {
    }

    /**
     * Calls {@code callable.call()} until it does not throw an exception (but no more than {@code retries} times).
     * <p>
     * Note that {@code callable} should be an idempotent operation which is a call to the AWS Service.
     * <p>
     * If {@code callable} throws an unchecked exception, it is wrapped into {@link HazelcastException}.
     */
    public static <T> T retry(Callable<T> callable, int retries) {
        int retryCount = 0;
        while (true) {
            try {
                return callable.call();
            } catch (Exception e) {
                retryCount++;
                if (retryCount > retries) {
                    throw ExceptionUtil.rethrow(e);
                }
                LOGGER.warning(String.format("Couldn't connect to the AWS service, %s retrying...", retryCount));
            }
        }
    }
}
