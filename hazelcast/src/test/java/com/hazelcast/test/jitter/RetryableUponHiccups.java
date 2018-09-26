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

package com.hazelcast.test.jitter;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Marks a test as retryable
 *
 * When a test fails while Jitter monitor is enabled and hiccups are detected, it will retried based on the options passed in
 * this annotation. On failures that hiccups where not detected, or where not severe enough to cross the {@link #thresholdMs()}
 * option, there will be no retry attempts.
 */
@Retention(RUNTIME)
@Target(ElementType.METHOD)
public @interface RetryableUponHiccups {
    /**
     * @return The max allowed accumulated pauses during this test before it avails of a retry
     */
    int thresholdMs() default 1000;

    /**
     * @return The number of retries before the test is marked as failed.
     */
    int retries() default 3;

    /**
     * @return The cool down period between retries, to maximize chances for a more stable environment during the next cycle
     */
    int coolDownPeriodMs() default 5000;
}
