/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.TestUtil;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DiagnosticsNonDynamicPluginTests extends AbstractDiagnosticsPluginTest {
    private HazelcastInstance hz;
    Map<String, String> properties = new ConcurrentHashMap<>();
    Diagnostics diagnostics;
    // source of truth for expected configs
    HazelcastProperties expectedHzProperties;

    @Before
    public void setup() {

        properties.put(StoreLatencyPlugin.PERIOD_SECONDS.getName(), "10");
        properties.put(StoreLatencyPlugin.RESET_PERIOD_SECONDS.getName(), "15");
        properties.put(Diagnostics.ENABLED.getName(), "true");

        Config config = new Config();
        properties.forEach(config::setProperty);

        Properties sourceProps = new Properties();
        sourceProps.putAll(properties);
        expectedHzProperties = new HazelcastProperties(sourceProps);

        hz = createHazelcastInstance(config);
        diagnostics = TestUtil.getNode(hz).getNodeEngine().getDiagnostics();
    }

    @Test
    public void testOnlyStaticLifecyclePlugins_runsOnStaticEnablement() {
        assertTrue(diagnostics.isEnabled());
        assertStoreLatencyPlugin(true);
        Assert.assertThrows(IllegalStateException.class, () -> {
            diagnostics.setConfig(new DiagnosticsConfig().setEnabled(false));
        });
        assertStoreLatencyPlugin(true);
        assertTrue(diagnostics.isEnabled());
    }

    void assertStoreLatencyPlugin(boolean status) {
        StoreLatencyPlugin plugin = diagnostics.getPlugin(StoreLatencyPlugin.class);

        assertNotNull(plugin);

        HazelcastProperty period = new HazelcastProperty(StoreLatencyPlugin.PERIOD_SECONDS.getName(),
                Integer.valueOf(properties.get(StoreLatencyPlugin.PERIOD_SECONDS.getName())),
                TimeUnit.SECONDS);

        HazelcastProperty resetPeriod = new HazelcastProperty(StoreLatencyPlugin.RESET_PERIOD_SECONDS.getName(),
                Integer.valueOf(properties.get(StoreLatencyPlugin.RESET_PERIOD_SECONDS.getName())),
                TimeUnit.SECONDS);

        assertEquals(status, plugin.isActive());
        assertEquals(expectedHzProperties.getMillis(period), plugin.getPeriodMillis());
        assertEquals(expectedHzProperties.getMillis(resetPeriod), plugin.getResetPeriodMillis());

        if (plugin.isActive()) {
            assertNotNull(diagnostics.getFutureOf(StoreLatencyPlugin.class));
        } else {
            assertNull(diagnostics.getFutureOf(StoreLatencyPlugin.class));
        }
    }

}
