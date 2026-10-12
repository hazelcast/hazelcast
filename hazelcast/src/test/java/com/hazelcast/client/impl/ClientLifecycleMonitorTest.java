/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl;

import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static com.hazelcast.spi.properties.ClusterProperty.CLIENT_CLEANUP_TIMEOUT;
import static com.hazelcast.test.HazelcastTestSupport.getFieldValue;
import static org.assertj.core.api.Assertions.assertThat;

@Category({QuickTest.class, ParallelJVMTest.class})
class ClientLifecycleMonitorTest {

    @Test
    void readsCleanupTimeoutInMilliseconds() {
        Properties properties = new Properties();
        properties.setProperty(CLIENT_CLEANUP_TIMEOUT.getName(), "10000");

        ClientLifecycleMonitor monitor = new ClientLifecycleMonitor(
                null, null, null, null, null, new HazelcastProperties(properties));
        long timeoutMillis = getFieldValue(monitor, "timeoutMillis");

        assertThat(timeoutMillis).isEqualTo(10000L);
    }
}
