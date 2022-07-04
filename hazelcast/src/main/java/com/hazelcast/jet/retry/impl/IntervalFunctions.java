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

package com.hazelcast.jet.retry.impl;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.retry.IntervalFunction;

public final class IntervalFunctions {

    private IntervalFunctions() {
    }

    public static IntervalFunction constant(long intervalMillis) {
        checkInterval(intervalMillis);
        return new IntervalFunctionImpl(attempt -> intervalMillis,
                "constant of " + intervalMillis + "ms");
    }

    public static IntervalFunction exponentialBackoff(long intervalMillis, double multiplier) {
        checkInterval(intervalMillis);
        checkMultiplier(multiplier);
        return new IntervalFunctionImpl(attempt -> (long) (intervalMillis * Math.pow(multiplier, attempt - 1)),
                "exponential backoff from " + intervalMillis + "ms by a factor of " + multiplier);
    }

    public static IntervalFunction exponentialBackoffWithCap(long intervalMillis, double multiplier, long capMillis) {
        checkInterval(intervalMillis);
        checkMultiplier(multiplier);
        checkInterval(capMillis);
        return new IntervalFunctionImpl(
                attempt -> Math.min(capMillis, (long) (intervalMillis * Math.pow(multiplier, attempt - 1))),
                "exponential backoff from " + intervalMillis + "ms by a factor of " + multiplier);
    }

    private static void checkInterval(long interval) {
        if (interval < 1) {
            throw new IllegalArgumentException(
                    "Illegal argument interval: " + interval + " millis is less than 1");
        }
    }

    private static void checkAttempt(Integer attempt) {
        if (attempt < 1) {
            throw new IllegalArgumentException("Illegal attempt argument: " + attempt);
        }
    }

    private static void checkMultiplier(double multiplier) {
        if (multiplier < 1.0) {
            throw new IllegalArgumentException("Illegal argument multiplier: " + multiplier);
        }
    }

    private static class IntervalFunctionImpl implements IntervalFunction {

        private static final long serialVersionUID = 1L;

        private final FunctionEx<Integer, Long> fn;
        private final String description;

        IntervalFunctionImpl(FunctionEx<Integer, Long> fn, String description) {
            this.fn = fn;
            this.description = description;
        }

        @Override
        public long waitAfterAttempt(int attempt) {
            checkAttempt(attempt);
            return fn.apply(attempt);
        }

        @Override
        public String toString() {
            return IntervalFunction.class.getSimpleName() + " (" + description + ")";
        }
    }
}
