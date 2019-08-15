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

package com.hazelcast.client.config;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import static com.hazelcast.nio.IOUtil.closeResource;
import static java.io.File.createTempFile;

public abstract class AbstractClientConfigImportVariableReplacementTest extends HazelcastTestSupport {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testImportElementOnlyAppearsInTopLevel() throws IOException;

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testHazelcastElementOnlyAppearsOnce();

    @Test
    public abstract void readVariables();

    @Test
    public abstract void testImportConfigFromResourceVariables() throws IOException;

    @Test
    public abstract void testImportedConfigVariableReplacement() throws IOException;

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testTwoResourceCyclicImportThrowsException() throws Exception;

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testThreeResourceCyclicImportThrowsException() throws Exception;

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testImportEmptyResourceContent() throws Exception;

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testImportEmptyResourceThrowsException();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testImportNotExistingResourceThrowsException();

    @Test
    public abstract void testReplacers() throws Exception;

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testMissingReplacement();

    @Test
    public abstract void testReplacerProperties();

    /**
     * Given: No replacer is used in the configuration file<br>
     * When: A property variable is used within the file<br>
     * Then: The configuration parsing doesn't fail and the variable string remains unchanged (i.e. backward compatible
     * behavior, as if {@code fail-if-value-missing} attribute is {@code false}).
     */
    @Test
    public abstract void testNoConfigReplacersMissingProperties();

    @Test
    public abstract void testImportGroupConfigFromClassPath();

    @Test
    public abstract void testReplaceVariablesUseSystemProperties() throws Exception;

    @Test
    public abstract void testReplaceVariablesWithClasspathConfig();

    protected static File createConfigFile(String filename, String suffix) throws IOException {
        File file = createTempFile(filename, suffix);
        file.setWritable(true);
        file.deleteOnExit();
        return file;
    }

    protected static void writeStringToStreamAndClose(FileOutputStream os, String string) throws IOException {
        os.write(string.getBytes());
        os.flush();
        closeResource(os);
    }
}
