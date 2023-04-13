/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.dataconnection.impl.hazelcastdataconnection;

import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.dataconnection.HazelcastDataConnection;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class HazelcastDataConnectionConfigLoaderTest {

    @Test
    public void testNothingLoaded() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig();
        dataConnectionConfig.setProperty(HazelcastDataConnection.CLIENT_XML, "xml");

        HazelcastDataConnectionConfigLoader loader = new HazelcastDataConnectionConfigLoader();
        DataConnectionConfig loadedConfig = loader.load(dataConnectionConfig);

        assertThat(loadedConfig.getProperty(HazelcastDataConnection.CLIENT_XML)).isEqualTo("xml");
        assertNull(loadedConfig.getProperty(HazelcastDataConnection.CLIENT_YML));
    }

    @Test
    public void testLoadXml() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig();
        Path path = Paths.get("src", "test", "resources", "hazelcast-client-test-external.xml");
        dataConnectionConfig.setProperty(HazelcastDataConnection.CLIENT_XML_PATH, path.toString());

        HazelcastDataConnectionConfigLoader loader = new HazelcastDataConnectionConfigLoader();
        DataConnectionConfig loadedConfig = loader.load(dataConnectionConfig);

        assertNotNull(loadedConfig.getProperty(HazelcastDataConnection.CLIENT_XML));
        assertNull(loadedConfig.getProperty(HazelcastDataConnection.CLIENT_YML));
    }

    @Test
    public void testLoadYml() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig();
        Path path = Paths.get("src", "test", "resources", "hazelcast-client-test-external.yaml");
        dataConnectionConfig.setProperty(HazelcastDataConnection.CLIENT_YML_PATH, path.toString());

        HazelcastDataConnectionConfigLoader loader = new HazelcastDataConnectionConfigLoader();
        DataConnectionConfig loadedConfig = loader.load(dataConnectionConfig);

        assertNotNull(loadedConfig.getProperty(HazelcastDataConnection.CLIENT_YML));
        assertNull(loadedConfig.getProperty(HazelcastDataConnection.CLIENT_XML));
    }
}

