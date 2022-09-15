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

package com.hazelcast.config;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.memory.Capacity;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Collections;
import java.util.Properties;

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

    static final String HAZELCAST_START_TAG = "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n";
    static final String HAZELCAST_END_TAG = "</hazelcast>\n";

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
    public void testExternalConfigOverrides() {
        String randomPropertyName = randomName();
        System.setProperty("hz.properties." + randomPropertyName, "123");

        Config cfg = Config.load();

        assertEquals("123", cfg.getProperty(randomPropertyName));
    }

    @Test
    public void testLoadFromString() {
        String xml = getSimpleXmlConfigStr(
                "instance-name", "hz-instance-name",
                "cluster-name", "${cluster.name}"
        );

        String yaml = getSimpleYamlConfigStr(
                "instance-name", "hz-instance-name",
                "cluster-name", "${cluster.name}"
        );

        String clusterName = randomName();
        Properties properties = new Properties();
        properties.setProperty("cluster.name", clusterName);

        Config cfg = Config.loadFromString(xml, properties);

        assertEquals(clusterName, cfg.getClusterName());
        assertEquals("hz-instance-name", cfg.getInstanceName());

        clusterName = randomName();
        properties.setProperty("cluster.name", clusterName);
        cfg = Config.loadFromString(yaml, properties);

        assertEquals(clusterName, cfg.getClusterName());
        assertEquals("hz-instance-name", cfg.getInstanceName());

        // test for very long config string with length > 4K
        final int longStrLength = 1 << 14;
        String instanceName = String.join("", Collections.nCopies(longStrLength, "a"));
        yaml = getSimpleYamlConfigStr("instance-name", instanceName);

        cfg = Config.loadFromString(yaml);
        assertEquals(instanceName, cfg.getInstanceName());
    }


    @Test
    public void testLoadEmptyXmlFromStream() {
        InputStream emptyXmlFromStream = new ByteArrayInputStream(getSimpleXmlConfigStr().getBytes());
        String randomPropertyName = randomName();
        System.setProperty("hz.properties." + randomPropertyName, "123");
        Config cfg = Config.loadFromStream(emptyXmlFromStream);
        assertEquals("123", cfg.getProperty(randomPropertyName));
        assertEquals(new Config().getMapConfig("default").getAsyncBackupCount(),
                cfg.getMapConfig("default").getAsyncBackupCount());
    }

    @Test
    public void testLoadEmptyYamlFromStream() {
        InputStream emptyYamlStream = new ByteArrayInputStream(getSimpleYamlConfigStr().getBytes());
        String randomPropertyName = randomName();
        System.setProperty("hz.properties." + randomPropertyName, "123");
        Config cfg = Config.loadFromStream(emptyYamlStream);
        assertEquals("123", cfg.getProperty(randomPropertyName));
        assertEquals(new Config().getMapConfig("default").getAsyncBackupCount(),
                cfg.getMapConfig("default").getAsyncBackupCount());
    }

    @Test
    public void testLoadFromStream() {
        InputStream xmlStream = new ByteArrayInputStream(
                getSimpleXmlConfigStr(
                        "instance-name", "hz-instance-name",
                        "cluster-name", "${cluster.name}"
                ).getBytes()
        );

        InputStream yamlStream = new ByteArrayInputStream(
                getSimpleYamlConfigStr(
                        "instance-name", "hz-instance-name",
                        "cluster-name", "${cluster.name}"
                ).getBytes()
        );

        String clusterName = randomName();
        Properties properties = new Properties();
        properties.setProperty("cluster.name", clusterName);

        Config cfg = Config.loadFromStream(xmlStream, properties);

        assertEquals(clusterName, cfg.getClusterName());
        assertEquals("hz-instance-name", cfg.getInstanceName());

        clusterName = randomName();
        properties.setProperty("cluster.name", clusterName);
        cfg = Config.loadFromStream(yamlStream, properties);

        assertEquals(clusterName, cfg.getClusterName());
        assertEquals("hz-instance-name", cfg.getInstanceName());

        // test for stream with > 4KB content
        final int instanceNameLen = 1 << 14;
        String instanceName = String.join("", Collections.nCopies(instanceNameLen, "x"));
        // wrap with BufferedInputStream (which is not resettable), so that ConfigStream
        // behaviour kicks in.
        yamlStream = new BufferedInputStream(new ByteArrayInputStream(
                getSimpleYamlConfigStr("instance-name", instanceName).getBytes())
        );

        cfg = Config.loadFromStream(yamlStream);
        assertEquals(instanceName, cfg.getInstanceName());
    }

    @Test
    public void testLoadFromFile() throws IOException {
        File file = createTempFile("foo", "cfg.xml");
        file.deleteOnExit();

        String randStr = randomString();
        String xml = getSimpleXmlConfigStr("license-key", randStr);

        Writer writer = new PrintWriter(file);
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

    @Test(expected = IllegalArgumentException.class)
    public void testSetConfigPropertyNameNull() {
        config.setProperty(null, "test");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetConfigPropertyNameEmpty() {
        config.setProperty(" ", "test");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetConfigPropertyValueNull() {
        config.setProperty("test", null);
    }

    @Test
    public void testGetDeviceConfig() {
        String deviceName = randomName();
        DeviceConfig deviceConfig = new LocalDeviceConfig().setName(deviceName);
        config.addDeviceConfig(deviceConfig);

        assertNull(config.getDeviceConfig(randomName()));
        assertEquals(deviceConfig, config.getDeviceConfig(deviceName));
        assertEquals(deviceConfig, config.getDeviceConfig(LocalDeviceConfig.class, deviceName));

        deviceConfig = new DeviceConfig() {
            @Override
            public boolean isLocal() {
                return false;
            }

            @Override
            public Capacity getCapacity() {
                return LocalDeviceConfig.DEFAULT_CAPACITY;
            }

            @Override
            public NamedConfig setName(String name) {
                return this;
            }

            @Override
            public String getName() {
                return deviceName;
            }
        };

        config.addDeviceConfig(deviceConfig);
        assertEquals(deviceConfig, config.getDeviceConfig(deviceName));
        assertEquals(deviceConfig, config.getDeviceConfig(DeviceConfig.class, deviceName));

        assertThrows(ClassCastException.class, () -> config.getDeviceConfig(LocalDeviceConfig.class, deviceName));
    }

    private static String getSimpleXmlConfigStr(String ...tagAndVal) {
        if (tagAndVal.length % 2 != 0) {
            throw new IllegalArgumentException("The number of tags and values parameters is odd."
                    + " Please provide these tags and values as pairs.");
        }
        StringBuilder sb = new StringBuilder();
        sb.append(HAZELCAST_START_TAG);

        for (int i = 0; i < tagAndVal.length - 1; i += 2) {
            sb.append("<" + tagAndVal[i] + ">" + tagAndVal[i + 1] + "</" + tagAndVal[i] + ">\n");
        }
        sb.append(HAZELCAST_END_TAG);
        return sb.toString();
    }

    private static String getSimpleYamlConfigStr(String ...tagAndVal) {
        if (tagAndVal.length % 2 != 0) {
            throw new IllegalArgumentException("The number of tags and values parameters is odd."
                    + " Please provide these tags and values as pairs.");
        }
        StringBuilder sb = new StringBuilder();
        sb.append("hazelcast:\n");

        for (int i = 0; i < tagAndVal.length - 1; i += 2) {
            sb.append("  " + tagAndVal[i] + ": " + tagAndVal[i + 1] + "\n");
        }
        return sb.toString();
    }
}
