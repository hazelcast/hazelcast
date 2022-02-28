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

package com.hazelcast.client.test.bounce;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.bounce.BounceMemberRule;
import com.hazelcast.test.bounce.BounceTestConfiguration;
import com.hazelcast.test.bounce.DriverFactory;

import static com.hazelcast.test.HazelcastTestSupport.waitAllForSafeState;

/**
 * Abstract client driver factory with customizable client config.
 */
public abstract class AbstractClientDriverFactory implements DriverFactory {

    protected ClientConfig clientConfig;

    public AbstractClientDriverFactory() {
    }

    public AbstractClientDriverFactory(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
    }

    @Override
    public HazelcastInstance[] createTestDrivers(BounceMemberRule rule) {
        BounceTestConfiguration testConfiguration = rule.getBounceTestConfig();
        switch (testConfiguration.getDriverType()) {
            case CLIENT:
                HazelcastInstance[] drivers = new HazelcastInstance[testConfiguration.getDriverCount()];
                for (int i = 0; i < drivers.length; i++) {
                    drivers[i] = ((TestHazelcastFactory) rule.getFactory())
                            .newHazelcastClient(getClientConfig(rule.getSteadyMember()));
                }
                waitAllForSafeState(drivers);
                return drivers;
            default:
                throw new AssertionError(
                        "ClientDriverFactory cannot create test drivers for " + testConfiguration.getDriverType());
        }
    }

    /**
     * Creates or processes an existing client config, given the cluster's steady member {@code HazelcastInstance}.
     */
    protected abstract ClientConfig getClientConfig(HazelcastInstance member);
}
