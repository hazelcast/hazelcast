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

package com.hazelcast.internal.util;

import java.util.concurrent.atomic.AtomicLong;

import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Responsible for tracking if concurrency is detected.
 * <p>
 * Concurrency is currently triggered at 2 levels:
 * - number of concurrent invocations
 * - contention on the io system.
 */
public abstract class ConcurrencyDetection {

    private final boolean enabled;

    private ConcurrencyDetection(boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * Checks if any concurrency is detected.
     * <p>
     * This call is thread-safe.
     *
     * @return true if detected, false otherwise.
     */
    public abstract boolean isDetected();

    /**
     * Called when concurrency is detected.
     * <p>
     * This call is thread-safe.
     */
    public abstract void onDetected();

    /**
     * Checks if this ConcurrencyDetection is enabled.
     * <p>
     * This is useful if you want to fall back to legacy mode.
     *
     * @return true if enabled, false otherwise.
     */
    public boolean enabled() {
        return enabled;
    }

    public static ConcurrencyDetection createEnabled(long durationMs) {
        return new EnabledConcurrencyDetection(durationMs);
    }

    public static ConcurrencyDetection createDisabled() {
        return new DisabledConcurrencyDetection();
    }

    /**
     * The DisabledConcurrencyDetection indicates that there is always concurrency,
     * even if there is no concurrency. This prevent write through and therefore lets the
     * system behave as before the write through was added.
     */
    private static final class DisabledConcurrencyDetection extends ConcurrencyDetection {

        private DisabledConcurrencyDetection() {
            super(false);
        }

        @Override
        public boolean isDetected() {
            return true;
        }

        @Override
        public void onDetected() {
            //no-op
        }
    }

    private static final class EnabledConcurrencyDetection extends ConcurrencyDetection {

        private final long windowNanos;
        private final AtomicLong expirationNanosRef = new AtomicLong(System.nanoTime());
        private final long halfWindowNanos;

        private EnabledConcurrencyDetection(long delayMs) {
            super(true);
            this.windowNanos = MILLISECONDS.toNanos(delayMs);
            this.halfWindowNanos = windowNanos / 2;
        }

        @Override
        public boolean isDetected() {
            return nanoTime() - expirationNanosRef.get() < 0;
        }

        @Override
        public void onDetected() {
            long nowNanos = nanoTime();
            long expirationNanos = expirationNanosRef.get();

            if (nowNanos - (expirationNanos - halfWindowNanos) > 0) {
                expirationNanosRef.lazySet(nowNanos + windowNanos);
            }
        }
    }
}
