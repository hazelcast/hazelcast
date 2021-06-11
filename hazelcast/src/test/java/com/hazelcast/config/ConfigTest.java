/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Properties;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.instance.ProtocolType.WAN;

import static java.io.File.createTempFile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@SuppressWarnings("checkstyle:Indentation")
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConfigTest extends HazelcastTestSupport {

    private Config config;

    @Before
    public void before() {
        config = new Config();
    }

    /**
     * Tests that the order of configuration creation matters.
     * <ul>
     * <li>Configurations which are created before the "default" configuration do not inherit from it.</li>
     * <li>Configurations which are created after the "default" configuration do inherit from it.</li>
     * </ul>
     */
    @Test
    public void testInheritanceFromDefaultConfig() {
        assertNotEquals("Expected that the default in-memory format is not OBJECT",
          MapConfig.DEFAULT_IN_MEMORY_FORMAT, InMemoryFormat.OBJECT);

        config.getMapConfig("myBinaryMap")
          .setBackupCount(3);
        config.getMapConfig("default")
          .setInMemoryFormat(InMemoryFormat.OBJECT);
        config.getMapConfig("myObjectMap")
          .setBackupCount(5);

        HazelcastInstance hz = createHazelcastInstance(config);

        MapConfig binaryMapConfig = hz.getConfig().findMapConfig("myBinaryMap");
        assertEqualsStringFormat("Expected %d sync backups, but found %d", 3, binaryMapConfig.getBackupCount());
        assertEqualsStringFormat("Expected %s in-memory format, but found %s",
          MapConfig.DEFAULT_IN_MEMORY_FORMAT, binaryMapConfig.getInMemoryFormat());

        MapConfig objectMapConfig = hz.getConfig().findMapConfig("myObjectMap");
        assertEqualsStringFormat("Expected %d sync backups, but found %d", 5, objectMapConfig.getBackupCount());
        assertEqualsStringFormat("Expected %s in-memory format, but found %s",
          InMemoryFormat.OBJECT, objectMapConfig.getInMemoryFormat());
    }


    @Test
    public void testLoadOverridesUsingExternalVariables() {
        String clusterName = randomName();
        System.setProperty("hz.cluster-name", clusterName);
        Config cfg = Config.load();

        assertEquals("load must apply external configuration (system / env. variable) overrides",
                clusterName, cfg.getClusterName());
    }

    @Test
    public void testLoadDefaultDoNotOverrideFromSystemVariables() {
        String clusterName = randomName();
        System.setProperty("hz.cluster-name", clusterName);
        Config cfg = Config.loadDefault();

        assertNotEquals(clusterName, cfg.getClusterName());
    }

    @Test
    public void testLoadYamlFromString() {
        String yaml = getSimpleYamlConfigStr(
                "instance-name", "hz-instance-name",
                "cluster-name", "${cluster.name}"
        );

        Properties properties = new Properties();
        properties.setProperty("cluster.name", "hz-cluster-name");

        Config cfg = Config.loadYamlFromString(yaml, properties);

        assertEquals("hz-instance-name", cfg.getInstanceName());
        assertEquals("hz-cluster-name", cfg.getClusterName());
    }

    @Test
    public void testLoadXmlFromString() {
        String xml = getSimpleXmlConfigStr(
                "instance-name", "hz-instance-name",
                "cluster-name", "${cluster.name}"
        );

        Properties properties = new Properties();
        properties.setProperty("cluster.name", "hz-cluster-name");

        Config cfg = Config.loadXmlFromString(xml, properties);

        assertEquals("hz-instance-name", cfg.getInstanceName());
        assertEquals("hz-cluster-name", cfg.getClusterName());
    }

    @Test
    public void testLoadFromFile() throws IOException {
        File file = createTempFile("foo", "cfg.xml");
        file.deleteOnExit();

        String randStr = randomString();
        String xml = getSimpleXmlConfigStr("license-key", randStr);

        Writer writer = new PrintWriter(file, "UTF-8");
        writer.write(xml);
        writer.close();

        Config cfg = Config.loadFromFile(file);
        assertEquals(randStr, cfg.getLicenseKey());
    }

    @Test
    public void testReturnNullMapConfig_whenThereIsNoMatch() {
        MapConfig mapConfig = new MapConfig("hz-map");

        config.addMapConfig(mapConfig);
        assertNotNull(config.getMapConfigOrNull("hz-map"));
        assertNull(config.getMapConfigOrNull("@invalid"));
    }

    @Test
    public void testReturnNullCacheConfig_whenThereIsNoMatch() {
        CacheSimpleConfig cacheConfig = new CacheSimpleConfig();
        cacheConfig.setName("hz-cache");

        config.addCacheConfig(cacheConfig);
        assertNotNull(config.findCacheConfigOrNull("hz-cache"));
        assertNull(config.findCacheConfigOrNull("@invalid"));
    }

    @Test
    public void testQueueConfigReturnDefault_whenThereIsNoMatch() {
        QueueConfig queueConfig = config.findQueueConfig("test");
        assertEquals("default", queueConfig.getName());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConfigThrow_whenConfigPatternMatcherIsNull() {
        config.setConfigPatternMatcher(null);
    }

    @Test
    public void testEndpointConfig() {
        String name = randomName();
        EndpointQualifier qualifier = EndpointQualifier.resolve(WAN, name);
        ServerSocketEndpointConfig endpointConfig = new ServerSocketEndpointConfig();
        endpointConfig.setName(name);
        endpointConfig.setProtocolType(WAN);
        config.getAdvancedNetworkConfig().addWanEndpointConfig(endpointConfig);

        assertEquals(endpointConfig,
          config.getAdvancedNetworkConfig().getEndpointConfigs().get(qualifier));
    }

    @Test
    public void testProgrammaticConfigGetUrlAndGetFileReturnNull() {
        Config config = new Config();
        assertNull(config.getConfigurationUrl());
        assertNull(config.getConfigurationFile());
    }

    private static String getSimpleXmlConfigStr(String ...tagAndVal) {
        if (tagAndVal.length == 0 || tagAndVal.length % 2 != 0) {
            throw new IllegalArgumentException("provide one or more tag and value pairs");
        }

        StringBuilder sb = new StringBuilder();
        sb.append("<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n");

        for (int i = 0; i < tagAndVal.length - 1; i += 2) {
            sb.append("<" + tagAndVal[i] + ">" + tagAndVal[i + 1] + "</" + tagAndVal[i] + ">\n");
        }
        sb.append("</hazelcast>\n");
        return sb.toString();
    }

    private static String getSimpleYamlConfigStr(String ...tagAndVal) {
        if (tagAndVal.length == 0 || tagAndVal.length % 2 != 0) {
            throw new IllegalArgumentException("provide one or more tag and value pairs");
        }
        StringBuilder sb = new StringBuilder();
        sb.append("hazelcast:\n");

        for (int i = 0; i < tagAndVal.length - 1; i += 2) {
            sb.append("  " + tagAndVal[i] + ": " + tagAndVal[i + 1] + "\n");
        }
        return sb.toString();
    }
}
