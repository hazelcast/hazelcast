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

package com.hazelcast.test;

import com.hazelcast.internal.util.Clock;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manageable {@link Clock.ClockImpl} implementation for tests.
 * <p>
 * Supports 3 clock modes:
 * <ul>
 * <li>SYSTEM: Using {@link System#currentTimeMillis()} as the time source.
 *
 * <li>MANAGED: The clock can be managed programmatically by advancing (or rewinding)
 * the time from a timestamp taken on switching to managed mode. After switching to
 * MANAGED mode, all subsequent {@link #currentTimeMillis()} calls return the same
 * timestamp either until the clock is shifted or {@link #unmanage()} is called.
 *
 * <li>SYNCHRONIZED: The clock is synchronized to a "master" clock. All subsequent
 * {@link #currentTimeMillis()} calls return the timestamp returned by a "master"
 * clock until {@link #unsync()} is called.
 * </ul>
 */
public class ManageableClock extends Clock.ClockImpl {

    private final AtomicReference<ClockSource> clockSourceRef = new AtomicReference<>(SystemClockSource.INSTANCE);

    /**
     * Sets this clock instance for the current thread. All child threads created
     * before a subsequent override for the current thread will inherit this clock.
     *
     * @return the clock instance
     */
    public ManageableClock useOnCurrentThreadInheritably() {
        ThreadLocalManageableClock.setClock(this);
        return this;
    }

    /**
     * Makes the clock programmatically managed until {@link #unmanage()} is called.
     *
     * @return the managed clock instance
     */
    public ManagedClock manage() {
        ManagedClock managedClock = new ManagedClock();
        clockSourceRef.set(managedClock.clockSource);
        return managedClock;
    }

    /**
     * Changes the clock back to SYSTEM mode. All subsequent {@link #currentTimeMillis()}
     * calls will return the system's time.
     *
     * @return the clock instance
     */
    public ManageableClock unmanage() {
        assert clockSourceRef.get() instanceof ManagedClockSource;

        clockSourceRef.set(SystemClockSource.INSTANCE);
        return this;
    }

    /**
     * Synchronizes the clock to the given {@code masterClock}. All subsequent
     * {@link #currentTimeMillis()} calls will return the same timestamp as the
     * {@code masterClock}'s {@link #currentTimeMillis()} method, until {@link #unsync()}
     * is called.
     *
     * @param masterClock The master clock to use
     * @return The synchronized clock instance
     */
    public SyncedClock syncTo(ManageableClock masterClock) {
        SyncedClock syncedClock = new SyncedClock();
        clockSourceRef.set(new SyncedClockSource(masterClock));
        return syncedClock;
    }

    /**
     * Changes the clock back to SYSTEM mode. All subsequent {@link #currentTimeMillis()}
     * calls will return the system's time.
     *
     * @return the clock instance
     */
    public ManageableClock unsync() {
        assert clockSourceRef.get() instanceof SyncedClockSource;

        clockSourceRef.set(SystemClockSource.INSTANCE);
        return this;
    }

    /**
     * Resets the clock to SYSTEM mode.
     *
     * @return the clock instance
     */
    ManageableClock reset() {
        clockSourceRef.set(SystemClockSource.INSTANCE);
        return this;
    }

    @Override
    protected long currentTimeMillis() {
        return clockSourceRef.get().currentTimeMillis();
    }

    private interface ClockSource {
        long currentTimeMillis();
    }

    private static final class SystemClockSource implements ClockSource {

        private static final SystemClockSource INSTANCE = new SystemClockSource();

        @Override
        public long currentTimeMillis() {
            return System.currentTimeMillis();
        }
    }

    private static final class ManagedClockSource implements ClockSource {

        private final AtomicLong timestamp = new AtomicLong(System.currentTimeMillis());

        @Override
        public long currentTimeMillis() {
            return timestamp.get();
        }
    }

    private static final class SyncedClockSource implements ClockSource {
        private final ManageableClock masterClock;

        private SyncedClockSource(ManageableClock masterClock) {
            this.masterClock = masterClock;
        }

        @Override
        public long currentTimeMillis() {
            return masterClock.currentTimeMillis();
        }
    }

    public final class ManagedClock extends Clock.ClockImpl {

        private final ManagedClockSource clockSource = new ManagedClockSource();

        /**
         * Advances the managed time by {@code delta} milliseconds.
         *
         * @param delta The delta to advance the time with
         * @return the managed clock instance
         */
        public ManagedClock advanceMillis(long delta) {
            assert clockSourceRef.get() instanceof ManagedClockSource;

            clockSource.timestamp.addAndGet(delta);
            return this;
        }

        /**
         * Convenience method for {@link #unmanage()}.
         *
         * @return the manageable clock instance
         */
        public ManageableClock unmanage() {
            ManageableClock.this.unmanage();
            return ManageableClock.this;
        }

        @Override
        protected long currentTimeMillis() {
            assert clockSourceRef.get() == clockSource;
            return clockSource.currentTimeMillis();
        }
    }

    public final class SyncedClock extends Clock.ClockImpl {


        /**
         * Convenience method for {@link #unsync()}.
         *
         * @return the manageable clock instance
         */
        public ManageableClock unsync() {
            return ManageableClock.this.unsync();
        }

        @Override
        protected long currentTimeMillis() {
            return clockSourceRef.get().currentTimeMillis();
        }
    }
}
