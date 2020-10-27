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

/**
 * Test clock implementation to be used for tests execution. The test clock
 * maintains an {@link InheritableThreadLocal} long value. When that value is
 * not set, then it returns the value of {@link System#currentTimeMillis()}.
 * When the thread local override value is set, then it returns that value
 * instead. This way the clock can stop, start, advance or go back at will,
 * by using the corresponding {@link #stop()}, {@link #delta(long)} and
 * {@link #start()} methods.
 *
 * Notice that the overriding long value is maintained in an {@link InheritableThreadLocal}
 * so that it is propagated to any threads that are started after the thread
 * local value has been created. Practically this means that for Hazelcast
 * operation threads to share the same overriding value as the test that
 * starts the {@code HazelcastInstance}, the thread local must be accessed
 * (eg by calling {@link #init()} before the {@code HazelcastInstance} is started.
 *
 * Caveats:
 * <ul>
 *     <li>
 *         Ensure a clock override is removed on test cleanup. Otherwise an override
 *         may remain and used in further tests executed by the same thread.
 *         TODO Not sure at all whether JUnit / our runners reuse threads or always
 *          create new ones???
 *     </li>
 * </ul>
 */
public class TestClock extends Clock.ClockImpl {

    private static final long CLOCK_OVERRIDE_UNSET = -1;

    static final InheritableThreadLocal<AtomicLong> THREAD_LOCAL_OVERRIDE =
            new InheritableThreadLocal<AtomicLong>() {
                @Override
                protected AtomicLong initialValue() {
                    return new AtomicLong(CLOCK_OVERRIDE_UNSET);
                }
            };

    @Override
    protected long currentTimeMillis() {
        long override = THREAD_LOCAL_OVERRIDE.get().get();
        return override == CLOCK_OVERRIDE_UNSET
                ? System.currentTimeMillis()
                : override;
    }

    public static void init() {
        THREAD_LOCAL_OVERRIDE.get();
    }

    public static void remove() {
        THREAD_LOCAL_OVERRIDE.remove();
    }

    public static void stop() {
        THREAD_LOCAL_OVERRIDE.get().set(System.currentTimeMillis());
    }

    public static void start() {
        THREAD_LOCAL_OVERRIDE.get().set(CLOCK_OVERRIDE_UNSET);
    }

    public static void delta(long delta) {
        assert THREAD_LOCAL_OVERRIDE.get().get() != CLOCK_OVERRIDE_UNSET
                : "delta can be only set on stopped clock";
        THREAD_LOCAL_OVERRIDE.get().getAndAdd(delta);
    }

    public static void deltaSeconds(long delta) {
        assert THREAD_LOCAL_OVERRIDE.get().get() != CLOCK_OVERRIDE_UNSET
                : "delta can be only set on stopped clock";
        THREAD_LOCAL_OVERRIDE.get().getAndAdd(delta * 1000);
    }
}
