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

import com.hazelcast.config.helpers.IOUtils;
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

    protected static File createConfigFile(String filename, String suffix) throws Exception {
        return IOUtils.createConfigFile(filename, suffix);
    }

    protected static void writeStringToStreamAndClose(FileOutputStream os, String string) throws Exception {
        IOUtils.writeStringToStreamAndClose(os, string);
    }

    static String createFileWithContent(String filename, String suffix, String content) throws Exception {
        File file = createConfigFile(filename, suffix);
        FileOutputStream os = new FileOutputStream(file);
        writeStringToStreamAndClose(os, content);
        return file.getAbsolutePath();
    }

    String createFilesWithCycleImports(String... paths) throws Exception {
        for (int i = 1; i < paths.length; i++) {
            createFileWithDependencyImport(paths[i - 1], paths[i]);
        }
        return createFileWithDependencyImport(paths[0], paths[1]);
    }

    private String createFileWithDependencyImport(
        String dependent,
        String pathToDependency) throws Exception {
        final String xmlContent = contentWithImportResource(pathToDependency);
        writeStringToStreamAndClose(new FileOutputStream(dependent), xmlContent);
        return xmlContent;
    }

    abstract String contentWithImportResource(String url);

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

    @Test
    public abstract void testImportNotExistingUrlResourceThrowsException();

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
    public abstract void testImportConfigFromClassPath();

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

    protected void expectHazelcastException() {
        rule.expect(HazelcastException.class);
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
