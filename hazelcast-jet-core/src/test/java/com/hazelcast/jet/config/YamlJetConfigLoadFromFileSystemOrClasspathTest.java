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

import com.hazelcast.jet.impl.util.Util;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static com.hazelcast.jet.config.AbstractJetConfigWithSystemPropertyTest.assertConfig;
import static com.hazelcast.jet.config.AbstractJetConfigWithSystemPropertyTest.assertDefaultMemberConfig;

public class YamlJetConfigLoadFromFileSystemOrClasspathTest extends AbstractJetConfigLoadFromFileSystemOrClasspathTest {

    public static final String TEST_YAML_JET = "hazelcast-jet-test.yaml";

    @Override
    public void when_fileSystemFileSpecified_usesSpecifiedFile() throws IOException {
        // Given
        File tempFile = File.createTempFile("jet", ".yaml");
        try (FileOutputStream os = new FileOutputStream(tempFile)) {
            InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream(TEST_YAML_JET);
            os.write(Util.readFully(resourceAsStream));
        }

        // When
        JetConfig jetConfig = JetConfig.loadFromFile(tempFile);

        // Then
        assertConfig(jetConfig);
        assertDefaultMemberConfig(jetConfig.getHazelcastConfig());
    }

    @Override
    @Test
    public void when_fileSystemPathSpecified_usesSpecifiedFile() throws IOException {
        // Given
        File tempFile = File.createTempFile("jet", ".yaml");
        try (FileOutputStream os = new FileOutputStream(tempFile)) {
            InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream(TEST_YAML_JET);
            os.write(Util.readFully(resourceAsStream));
        }

        // When
        JetConfig jetConfig = JetConfig.loadFromFile(tempFile);

        // Then
        assertConfig(jetConfig);
        assertDefaultMemberConfig(jetConfig.getHazelcastConfig());
    }

    @Override
    @Test
    public void when_classpathSpecified_usesSpecifiedResource() {
        // When
        JetConfig jetConfig = JetConfig.loadFromClasspath(getClass().getClassLoader(), TEST_YAML_JET);

        // Then
        assertConfig(jetConfig);
        assertDefaultMemberConfig(jetConfig.getHazelcastConfig());
    }

    @Override
    public void when_classpathSpecifiedWithClassloader_usesSpecifiedResource() {
        // When
        JetConfig jetConfig = JetConfig.loadFromClasspath(getClass().getClassLoader(), TEST_YAML_JET);

        // Then
        assertConfig(jetConfig);
        assertDefaultMemberConfig(jetConfig.getHazelcastConfig());
    }

}
