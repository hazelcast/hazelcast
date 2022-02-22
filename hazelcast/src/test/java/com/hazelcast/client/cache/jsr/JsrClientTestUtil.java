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

package com.hazelcast.client.cache.jsr;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;

import static com.hazelcast.cache.jsr.JsrTestUtil.assertNoMBeanLeftovers;
import static com.hazelcast.cache.jsr.JsrTestUtil.clearCachingProviderRegistry;
import static com.hazelcast.cache.jsr.JsrTestUtil.clearSystemProperties;
import static com.hazelcast.cache.jsr.JsrTestUtil.setSystemProperties;
import static com.hazelcast.test.HazelcastTestSupport.assertThatIsNotMultithreadedTest;

/**
 * Utility class responsible for setup/cleanup of client JSR tests.
 */
public final class JsrClientTestUtil {

    private JsrClientTestUtil() {
    }

    public static void setup() {
        assertThatIsNotMultithreadedTest();
        setSystemProperties("client");
    }

    public static void setupWithHazelcastInstance() {
        assertThatIsNotMultithreadedTest();
        setSystemProperties("client");
        assertNoMBeanLeftovers();

        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        Hazelcast.newHazelcastInstance(config);
    }

    public static void cleanup() {
        clearSystemProperties();
        clearCachingProviderRegistry();

        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
        HazelcastInstanceFactory.terminateAll();
    }
}
