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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.ConcurrentReferenceHashMap;
import com.hazelcast.internal.util.StringUtil;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import static com.hazelcast.internal.util.ClockProperties.HAZELCAST_CLOCK_IMPL;
import static com.hazelcast.internal.util.ConcurrentReferenceHashMap.ReferenceType.STRONG;
import static com.hazelcast.internal.util.ConcurrentReferenceHashMap.ReferenceType.WEAK;

/**
 * Clock rule to be used in all tests. The rule makes sure the used managed
 * clock gets reset to SYSTEM mode before and after a test case execution.
 *
 * @see ThreadLocalManageableClock
 * @see ManageableClock
 */
public class ManageableClockRule implements TestRule {
    private final ConcurrentReferenceHashMap<HazelcastInstance, ManageableClock> clocksMap =
            new ConcurrentReferenceHashMap<>(WEAK, STRONG);

    @Override
    public Statement apply(Statement base, Description description) {
        // ensure there is no other clock set
        assert StringUtil.isNullOrEmptyAfterTrim(System.getProperty(HAZELCAST_CLOCK_IMPL))
                || ThreadLocalManageableClock.class.getCanonicalName().equals(System.getProperty(HAZELCAST_CLOCK_IMPL));

        System.setProperty(HAZELCAST_CLOCK_IMPL, ThreadLocalManageableClock.class.getCanonicalName());

        return new Statement() {

            @Override
            public void evaluate() throws Throwable {
                ThreadLocalManageableClock.resetIfNonNull();
                try {
                    base.evaluate();
                } finally {
                    ThreadLocalManageableClock.resetIfNonNull();
                }
            }
        };
    }

    public ManageableClock clockOf(HazelcastInstance instance) {
        return clocksMap.get(instance);
    }

    HazelcastInstance createHazelcastInstanceWithClock(HazelcastInstanceFactoryFunction factoryFn) {
        ManageableClock currentClock = ThreadLocalManageableClock.THREAD_LOCAL_CLOCK.get();
        ManageableClock assignedClock = currentClock;

        if (currentClock == null) {
            // creating a dedicated new clock for the to be created hazelcast instance
            // this is the typical case
            assignedClock = new ManageableClock();
            ThreadLocalManageableClock.THREAD_LOCAL_CLOCK.set(assignedClock);
        }

        // this must be done after setting the thread local clock, since the clock
        // will be inherited by the threads of the new HazelcastInstance
        HazelcastInstance hazelcastInstance = factoryFn.create();

        if (currentClock == null) {
            // clear the thread-local clock
            ThreadLocalManageableClock.THREAD_LOCAL_CLOCK.set(null);
        }
        clocksMap.put(hazelcastInstance, assignedClock);

        return hazelcastInstance;
    }

    @FunctionalInterface
    public interface HazelcastInstanceFactoryFunction {
        HazelcastInstance create();
    }
}
