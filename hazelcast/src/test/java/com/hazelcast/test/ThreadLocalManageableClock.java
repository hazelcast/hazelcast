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

/**
 * An inheritable (by child threads) thread-local container
 * for the {@link ManageableClock} instances. Used to manage
 * the time in tests per-member or per-cluster basis.
 */
public class ThreadLocalManageableClock extends Clock.ClockImpl {
    static final InheritableThreadLocal<ManageableClock> THREAD_LOCAL_CLOCK =
            new InheritableThreadLocal<ManageableClock>() {
                @Override
                protected ManageableClock initialValue() {
                    return null;
                }
            };

    private static final ManageableClock FALLBACK_SYSTEM_CLOCK = new ManageableClock();

    static void resetIfNonNull() {
        ManageableClock clock = THREAD_LOCAL_CLOCK.get();
        if (clock != null) {
            clock.reset();
        }
    }

    static void setClock(ManageableClock clock) {
        THREAD_LOCAL_CLOCK.set(clock);
    }

    @Override
    protected long currentTimeMillis() {
        ManageableClock currentClock = THREAD_LOCAL_CLOCK.get();
        if (currentClock == null) {
            currentClock = FALLBACK_SYSTEM_CLOCK;
        }
        return currentClock.currentTimeMillis();
    }
}
