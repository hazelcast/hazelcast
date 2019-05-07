/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.config;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Abstract test class defining the common test cases for loading XML and YAML
 * based configuration file from system properties.
 *
 * @see XmlJetConfigWithSystemPropertyTest
 * @see YamlJetConfigWithSystemPropertyTest
 * @see XmlJetClientConfigWithSystemPropertyTest
 * @see YamlJetClientConfigWithSystemPropertyTest
 */
@RunWith(HazelcastSerialClassRunner.class)
public abstract class AbstractJetConfigWithSystemPropertyTest {

    protected static final String HAZELCAST_JET_CONFIG_PROPERTY = "hazelcast.jet.config";
    protected static final String HAZELCAST_MEMBER_CONFIG_PROPERTY = "hazelcast.config";
    protected static final String HAZELCAST_CLIENT_CONFIG_PROPERTY = "hazelcast.client.config";
    protected static final String TEST_GROUP_NAME = "imdg";
    protected static final String PASSWORD = "PASSWORD";
    protected static final String INSTANCE_NAME = "my-instance";

    @Before
    @After
    public void beforeAndAfter() {
        System.clearProperty(HAZELCAST_JET_CONFIG_PROPERTY);
        System.clearProperty(HAZELCAST_MEMBER_CONFIG_PROPERTY);
        System.clearProperty(HAZELCAST_CLIENT_CONFIG_PROPERTY);
    }

    @Test(expected = HazelcastException.class)
    public abstract void when_filePathSpecifiedNonExistingFile_thenThrowsException() throws Exception;

    @Test
    public abstract void when_filePathSpecified_usesSpecifiedFile() throws IOException;

    @Test(expected = HazelcastException.class)
    public abstract void when_classpathSpecifiedNonExistingFile_thenThrowsException();

    @Test
    public abstract void when_classpathSpecified_usesSpecifiedResource();

    @Test
    public abstract void when_configHasVariable_variablesAreReplaced();

    protected static void assertConfig(JetConfig jetConfig) {
        assertEquals("cooperativeThreadCount", 55, jetConfig.getInstanceConfig().getCooperativeThreadCount());
        assertEquals("backupCount", 2, jetConfig.getInstanceConfig().getBackupCount());
        assertEquals("flowControlMs", 50, jetConfig.getInstanceConfig().getFlowControlPeriodMs());
        assertEquals("scaleUpDelayMillis", 1234, jetConfig.getInstanceConfig().getScaleUpDelayMillis());
        assertFalse("losslessRestartEnabled", jetConfig.getInstanceConfig().isLosslessRestartEnabled());

        assertEquals("value1", jetConfig.getProperties().getProperty("property1"));
        assertEquals("value2", jetConfig.getProperties().getProperty("property2"));

        MetricsConfig metricsCfg = jetConfig.getMetricsConfig();
        assertFalse("isEnabled", metricsCfg.isEnabled());
        assertFalse("isJmxEnabled", metricsCfg.isJmxEnabled());
        assertEquals("metricsRetentionSeconds", 124, metricsCfg.getRetentionSeconds());
        assertEquals("metricsCollectionInterval", 123, metricsCfg.getCollectionIntervalSeconds());
        assertTrue("metricsForDataStructures", metricsCfg.isMetricsForDataStructuresEnabled());
    }

    protected static void assertDefaultMemberConfig(Config config) {
        assertThat(config, not(nullValue()));
        assertThat(config.getGroupConfig().getName(), not(equalTo(TEST_GROUP_NAME)));
    }

    protected static void assertMemberConfig(Config config) {
        assertThat(config, not(nullValue()));
        assertThat(config.getGroupConfig().getName(), equalTo(TEST_GROUP_NAME));
    }


    protected static void assertClientConfig(ClientConfig config) {
        assertThat(config, not(nullValue()));
        assertThat(config.getGroupConfig().getName(), equalTo(TEST_GROUP_NAME));
        assertThat(config.getGroupConfig().getPassword(), equalTo(PASSWORD));
        assertThat(config.getNetworkConfig().getAddresses(), hasItem("127.0.59.1:5701"));
    }

    protected static void assertDefaultClientConfig(ClientConfig config) {
        assertThat(config, not(nullValue()));
        assertThat(config.getGroupConfig().getName(), equalTo("jet"));
    }

}
