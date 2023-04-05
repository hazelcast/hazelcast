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

package com.hazelcast.datalink.impl.hazelcastdatalink;

import com.hazelcast.config.DataLinkConfig;
import com.hazelcast.datalink.HazelcastDataLink;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class HazelcastDataLinkConfigLoaderTest {

    @Test
    public void testNothingLoaded() {
        DataLinkConfig dataLinkConfig = new DataLinkConfig();
        dataLinkConfig.setProperty(HazelcastDataLink.CLIENT_XML, "xml");

        HazelcastDataLinkConfigLoader loader = new HazelcastDataLinkConfigLoader();
        DataLinkConfig loadedConfig = loader.load(dataLinkConfig);

        assertThat(loadedConfig.getProperty(HazelcastDataLink.CLIENT_XML)).isEqualTo("xml");
        assertNull(loadedConfig.getProperty(HazelcastDataLink.CLIENT_YML));
    }

    @Test
    public void testLoadXml() {
        DataLinkConfig dataLinkConfig = new DataLinkConfig();
        Path path = Paths.get("src", "test", "resources", "hazelcast-client-test-external.xml");
        dataLinkConfig.setProperty(HazelcastDataLink.CLIENT_XML_PATH, path.toString());

        HazelcastDataLinkConfigLoader loader = new HazelcastDataLinkConfigLoader();
        DataLinkConfig loadedConfig = loader.load(dataLinkConfig);

        assertNotNull(loadedConfig.getProperty(HazelcastDataLink.CLIENT_XML));
        assertNull(loadedConfig.getProperty(HazelcastDataLink.CLIENT_YML));
    }

    @Test
    public void testLoadYml() {
        DataLinkConfig dataLinkConfig = new DataLinkConfig();
        Path path = Paths.get("src", "test", "resources", "hazelcast-client-test-external.yaml");
        dataLinkConfig.setProperty(HazelcastDataLink.CLIENT_YML_PATH, path.toString());

        HazelcastDataLinkConfigLoader loader = new HazelcastDataLinkConfigLoader();
        DataLinkConfig loadedConfig = loader.load(dataLinkConfig);

        assertNotNull(loadedConfig.getProperty(HazelcastDataLink.CLIENT_YML));
        assertNull(loadedConfig.getProperty(HazelcastDataLink.CLIENT_XML));
    }
}

