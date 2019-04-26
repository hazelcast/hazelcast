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

import com.hazelcast.config.ConfigurationException;
import com.hazelcast.core.HazelcastException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Abstract class defining common test cases for {@link XmlJetConfigImportVariableReplacementTest}
 * and {@link YamlJetConfigImportVariableReplacementTest}
 */
public abstract class AbstractJetConfigImportVariableReplacementTest {

    @Rule
    public ExpectedException rule = ExpectedException.none();
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @SuppressWarnings("ResultOfMethodCallIgnored")
    protected static File createConfigFile(String filename, String suffix) throws Exception {
        File file = File.createTempFile(filename, suffix);
        file.setWritable(true);
        file.deleteOnExit();
        return file;
    }

    protected static void writeStringToStreamAndClose(FileOutputStream os, String string) throws Exception {
        os.write(string.getBytes());
        os.flush();
        os.close();
    }


    @Test
    public abstract void testHazelcastJetElementOnlyAppearsOnce();

    @Test
    public abstract void readVariables();

    @Test
    public abstract void testImportConfigFromResourceVariables() throws Exception;

    @Test
    public abstract void testImportedConfigVariableReplacement() throws Exception;

    @Test
    public abstract void testTwoResourceCyclicImportThrowsException() throws Exception;

    @Test
    public abstract void testThreeResourceCyclicImportThrowsException() throws Exception;

    @Test
    public abstract void testImportEmptyResourceContent() throws Exception;

    @Test
    public abstract void testImportEmptyResourceThrowsException();

    @Test
    public abstract void testImportNotExistingResourceThrowsException();

    @Test(expected = HazelcastException.class)
    public abstract void testImportFromNonHazelcastJetConfigThrowsException() throws Exception;

    @Test
    public abstract void testImportMetricsConfigFromFile() throws Exception;

    @Test
    public abstract void testImportInstanceConfigFromFile() throws Exception;

    @Test
    public abstract void testImportEdgeConfigFromFile() throws Exception;

    @Test
    public abstract void testReplacers() throws Exception;

    @Test(expected = ConfigurationException.class)
    public abstract void testMissingReplacement() throws Exception;

    @Test
    public abstract void testBadVariableSyntaxIsIgnored() throws Exception;

    @Test
    public abstract void testReplacerProperties() throws Exception;

    /**
     * Given: No replacer is used in the configuration file<br>
     * When: A property variable is used within the file<br>
     * Then: The configuration parsing doesn't fail and the variable string remains unchanged (i.e. backward compatible
     * behavior, as if {@code fail-if-value-missing} attribute is {@code false}).
     */
    @Test
    public abstract void testNoConfigReplacersMissingProperties() throws Exception;

    @Test
    public abstract void testVariableReplacementAsSubstring();

    @Test
    public abstract void testImportWithVariableReplacementAsSubstring() throws Exception;

    @Test
    public abstract void testReplaceVariablesWithFileSystemConfig() throws Exception;

    @Test
    public abstract void testReplaceVariablesWithInMemoryConfig();

    @Test
    public abstract void testReplaceVariablesWithClasspathConfig();

    @Test
    public abstract void testReplaceVariablesWithUrlConfig() throws Exception;

    @Test
    public abstract void testReplaceVariablesUseSystemProperties() throws Exception;

    protected void assertPropertiesOnConfig(JetConfig config) {
        assertEquals(123, config.getInstanceConfig().getCooperativeThreadCount());
        assertEquals(456, config.getInstanceConfig().getFlowControlPeriodMs());
        assertEquals(6, config.getInstanceConfig().getBackupCount());
        assertEquals(1234, config.getInstanceConfig().getScaleUpDelayMillis());
        assertTrue(config.getInstanceConfig().isLosslessRestartEnabled());

        MetricsConfig metricsConfig = config.getMetricsConfig();
        assertFalse(metricsConfig.isEnabled());
        assertFalse(metricsConfig.isJmxEnabled());
        assertEquals(123, metricsConfig.getCollectionIntervalSeconds());
        assertEquals(124, metricsConfig.getRetentionSeconds());
        assertTrue(metricsConfig.isMetricsForDataStructuresEnabled());
    }

    protected Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty("thread.count", "123");
        properties.setProperty("flow.control.period", "456");
        properties.setProperty("backup.count", "6");
        properties.setProperty("scale.up.delay.millis", "1234");
        properties.setProperty("lossless.restart.enabled", "true");

        properties.setProperty("metrics.enabled", "false");
        properties.setProperty("metrics.jmxEnabled", "false");
        properties.setProperty("metrics.retention", "124");
        properties.setProperty("metrics.collection-interval", "123");
        properties.setProperty("metrics.enabled-for-data-structures", "true");
        return properties;
    }


}
