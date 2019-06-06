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

package com.hazelcast.config;

import com.hazelcast.config.replacer.PropertyReplacer;
import com.hazelcast.config.replacer.spi.ConfigReplacer;
import com.hazelcast.core.HazelcastException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Properties;

/**
 * Abstract class defining common test cases for {@link XmlConfigImportVariableReplacementTest}
 * and {@link YamlConfigImportVariableReplacementTest}
 */
public abstract class AbstractConfigImportVariableReplacementTest {
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
    public abstract void testHazelcastElementOnlyAppearsOnce();

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
    public abstract void testImportFromNonHazelcastConfigThrowsException() throws Exception;

    @Test
    public abstract void testImportNetworkConfigFromFile() throws Exception;

    @Test
    public abstract void testImportMapConfigFromFile() throws Exception;

    /**
     * This test case verifies the behavior of the XML and YAML import
     * implementations when the definition of one map is spanned over
     * two configuration files. Note that there is a difference between
     * the two implementations. XML uses the content for the given map
     * from the main XML (where the import is), while YAML recursively
     * merges the main and the imported file. See the assertions in the
     * two test case implementations.
     */
    @Test
    public abstract void testImportOverlappingMapConfigFromFile() throws Exception;

    @Test
    public abstract void testMapConfigFromMainAndImportedFile() throws Exception;

    @Test
    public abstract void testImportGroupConfigFromClassPath();

    @Test
    public abstract void testReplacers() throws Exception;

    @Test(expected = InvalidConfigurationException.class)
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

    protected void expectInvalid() {
        InvalidConfigurationTest.expectInvalid(rule);
    }

    public static class IdentityReplacer implements ConfigReplacer {
        @Override
        public String getPrefix() {
            return "ID";
        }

        @Override
        public String getReplacement(String maskedValue) {
            return maskedValue;
        }

        @Override
        public void init(Properties properties) {
        }
    }

    public static class TestReplacer extends PropertyReplacer {
        @Override
        public String getPrefix() {
            return "T";
        }
    }
}
