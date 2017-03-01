/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.bounce;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;

import static com.hazelcast.test.HazelcastTestSupport.waitAllForSafeState;

/**
 * Default member-side test driver factory. When test driver is {@code ALWAYS_UP_MEMBER}, returns a single test driver
 * set to the always-up member of the cluster as returned by {@link BounceMemberRule#getSteadyMember()}. When test driver
 * is {@code MEMBER}, the configured number of test drivers are created. Otherwise, an {@code AssertionError} is thrown.
 */
public class MemberDriverFactory implements DriverFactory {

    @Override
    public HazelcastInstance[] createTestDrivers(BounceMemberRule rule) {
        BounceTestConfiguration testConfiguration = rule.getBounceTestConfig();
        switch (testConfiguration.getDriverType()) {
            case ALWAYS_UP_MEMBER:
                return new HazelcastInstance[] {rule.getSteadyMember()};
            case MEMBER:
                HazelcastInstance[] drivers = new HazelcastInstance[testConfiguration.getDriverCount()];
                for (int i = 0; i < drivers.length; i++) {
                    drivers[i] = rule.getFactory().newHazelcastInstance(getConfig());
                }
                waitAllForSafeState(drivers);
                return drivers;
            default:
                throw new AssertionError("MemberDriverFactory cannot create test drivers for "
                        + testConfiguration.getDriverType());
        }
    }

    /**
     * Override this method to provide custom configuration for test drivers
     * @return
     */
    protected Config getConfig() {
        return new Config();
    }
}
