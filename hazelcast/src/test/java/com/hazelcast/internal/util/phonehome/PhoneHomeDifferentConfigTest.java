/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.internal.util.phonehome;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.TieredStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.memory.Capacity;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.DATA_MEMORY_COST;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.HD_MEMORY_ENABLED;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.JET_ENABLED;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.JET_RESOURCE_UPLOAD_ENABLED;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.MEMORY_FREE_HEAP_SIZE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.MEMORY_USED_HEAP_SIZE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.MEMORY_USED_NATIVE_SIZE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.TIERED_STORAGE_ENABLED;
import static com.hazelcast.memory.MemoryUnit.MEGABYTES;
import static com.hazelcast.test.Accessors.getNode;
import static java.lang.System.getenv;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PhoneHomeDifferentConfigTest extends HazelcastTestSupport {
    private Map<String, String> parameters;

    private String get(Metric metric) {
        return parameters.get(metric.getQueryParameter());
    }

    @Test
    public void testScheduling_whenPhoneHomeIsDisabled() {
        Config config = new Config()
                .setProperty(ClusterProperty.PHONE_HOME_ENABLED.getName(), "false");

        HazelcastInstance hz = createHazelcastInstance(config);
        Node node = getNode(hz);

        PhoneHome phoneHome = new PhoneHome(node);

        phoneHome.start();
        assertNull(phoneHome.phoneHomeFuture);
    }

    @Test
    public void testShutdown() {
        assumeFalse("Skipping. The PhoneHome is disabled by the Environment variable",
                "false".equals(getenv("HZ_PHONE_HOME_ENABLED")));
        Config config = new Config()
                .setProperty(ClusterProperty.PHONE_HOME_ENABLED.getName(), "true");

        HazelcastInstance hz = createHazelcastInstance(config);
        Node node = getNode(hz);

        PhoneHome phoneHome = new PhoneHome(node);
        phoneHome.start();
        assertNotNull(phoneHome.phoneHomeFuture);
        assertFalse(phoneHome.phoneHomeFuture.isDone());
        assertFalse(phoneHome.phoneHomeFuture.isCancelled());

        phoneHome.shutdown();
        assertTrue(phoneHome.phoneHomeFuture.isCancelled());
    }

    @Test
    public void testJetDisabled() {
        Config config = new Config()
                .setJetConfig(new JetConfig().setEnabled(false));
        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        Node node = getNode(hazelcastInstance);

        PhoneHome phoneHome = new PhoneHome(node);

        parameters = phoneHome.phoneHome(true);
        assertEquals("false", get(JET_ENABLED));
        assertEquals("false", get(JET_RESOURCE_UPLOAD_ENABLED));
    }

    @Test
    public void testHdStorage() {
        NativeMemoryConfig nativeMemoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setCapacity(new Capacity(64L, MEGABYTES));
        Config config = new Config()
                .setNativeMemoryConfig(nativeMemoryConfig);
        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        Node node = getNode(hazelcastInstance);

        PhoneHome phoneHome = new PhoneHome(node);

        parameters = phoneHome.phoneHome(true);
        assertThat(get(HD_MEMORY_ENABLED)).isEqualTo("true");
        assertThat(get(MEMORY_USED_HEAP_SIZE)).isGreaterThan("0");
        assertThat(get(MEMORY_FREE_HEAP_SIZE)).isGreaterThan("0");
        assertThat(get(MEMORY_USED_NATIVE_SIZE)).isEqualTo("0");
        assertThat(get(TIERED_STORAGE_ENABLED)).isEqualTo("false");
        assertThat(get(DATA_MEMORY_COST)).isEqualTo("0");
    }

    @Test
    public void testTieredStorage() {
        MapConfig mapConfig = new MapConfig()
                .setName("ts-map")
                .setTieredStoreConfig(new TieredStoreConfig().setEnabled(true));
        Config config = new Config()
                .addMapConfig(mapConfig);
        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        Node node = getNode(hazelcastInstance);

        PhoneHome phoneHome = new PhoneHome(node);

        parameters = phoneHome.phoneHome(true);
        assertThat(get(TIERED_STORAGE_ENABLED)).isEqualTo("true");
        assertThat(get(HD_MEMORY_ENABLED)).isEqualTo("false");
    }
}
