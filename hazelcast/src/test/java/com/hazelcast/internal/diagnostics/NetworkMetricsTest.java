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

package com.hazelcast.internal.diagnostics;

import static com.hazelcast.test.OverridePropertyRule.set;
import static com.hazelcast.test.TestEnvironment.HAZELCAST_TEST_USE_NETWORK;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.OverridePropertyRule;

@RunWith(HazelcastParallelClassRunner.class)
public class NetworkMetricsTest extends DefaultMetricsTest {

    /**
     * In order to get the TCP statistics we need a real {@link ConnectionManager}.
     */
    @Rule
    public final OverridePropertyRule useRealNetwork = set(HAZELCAST_TEST_USE_NETWORK, "true");

    @Test
    public void connectionManagerStats() {
        assertHasStatsEventually(10, "tcp.connection.");
    }

    @Test
    public void healthMonitorMetrics() {
        assertHasAllStatsEventually(
                "tcp.connection.activeCount",
                "tcp.connection.count",
                "tcp.connection.clientCount");
    }

}
