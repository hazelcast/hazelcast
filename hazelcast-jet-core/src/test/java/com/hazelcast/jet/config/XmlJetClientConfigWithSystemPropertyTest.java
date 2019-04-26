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
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.jet.impl.config.XmlJetClientConfigLocator;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
public class XmlJetClientConfigWithSystemPropertyTest extends AbstractJetConfigWithSystemPropertyTest {

    private static final String JET_CLIENT_XML = "hazelcast-client-test.xml";
    private static final String JET_CLIENT_WITH_VARIABLES_XML = "hazelcast-jet-client-with-variables.xml";

    @Test
    public void when_locateDefault_thenLoadsDefault() throws Exception {
        // When
        XmlJetClientConfigLocator locator = new XmlJetClientConfigLocator();
        locator.locateDefault();
        ClientConfig clientConfig = new XmlClientConfigBuilder(locator.getIn()).build();

        //Then
        assertDefaultClientConfig(clientConfig);
    }


    @Override
    @Test(expected = HazelcastException.class)
    public void when_filePathSpecifiedNonExistingFile_thenThrowsException() throws Exception {
        // Given
        File file = File.createTempFile("foo", ".xml");
        file.delete();
        System.setProperty(HAZELCAST_CLIENT_CONFIG_PROPERTY, file.getAbsolutePath());

        // When
        XmlJetClientConfigLocator locator = new XmlJetClientConfigLocator();
        locator.locateEverywhere();
        new XmlClientConfigBuilder(locator.getIn()).build();
    }

    @Override
    @Test
    public void when_filePathSpecified_usesSpecifiedFile() throws IOException {
        //Given
        File tempFile = File.createTempFile("jet", ".xml");
        try (FileOutputStream os = new FileOutputStream(tempFile)) {
            InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream(JET_CLIENT_XML);
            os.write(Util.readFully(resourceAsStream));
        }
        System.setProperty(HAZELCAST_CLIENT_CONFIG_PROPERTY, tempFile.getAbsolutePath());

        //When
        XmlJetClientConfigLocator locator = new XmlJetClientConfigLocator();
        locator.locateEverywhere();
        ClientConfig config = new XmlClientConfigBuilder(locator.getIn()).build();


        //Then
        assertClientConfig(config);
    }


    @Override
    @Test(expected = HazelcastException.class)
    public void when_classpathSpecifiedNonExistingFile_thenThrowsException() {
        // Given
        System.setProperty(HAZELCAST_CLIENT_CONFIG_PROPERTY, "classpath:non-existing.xml");

        //When
        XmlJetClientConfigLocator locator = new XmlJetClientConfigLocator();
        new XmlClientConfigBuilder(locator.getIn()).build();
    }

    @Override
    @Test
    public void when_classpathSpecified_usesSpecifiedResource() {
        // Given
        System.setProperty(HAZELCAST_CLIENT_CONFIG_PROPERTY, "classpath:" + JET_CLIENT_XML);

        //When
        XmlJetClientConfigLocator locator = new XmlJetClientConfigLocator();
        locator.locateEverywhere();
        ClientConfig config = new XmlClientConfigBuilder(locator.getIn()).build();

        //Then
        assertClientConfig(config);
    }


    @Override
    @Test
    public void when_configHasVariable_variablesAreReplaced() {
        // Given
        System.setProperty(HAZELCAST_CLIENT_CONFIG_PROPERTY, "classpath:" + JET_CLIENT_WITH_VARIABLES_XML);
        Properties properties = new Properties();
        properties.setProperty("group.name", "test");
        properties.setProperty("group.pass", String.valueOf(1234));
        properties.setProperty("member", "19.0.0.2:5670");

        // When
        XmlJetClientConfigLocator locator = new XmlJetClientConfigLocator();
        locator.locateEverywhere();
        XmlClientConfigBuilder builder = new XmlClientConfigBuilder(locator.getIn());
        builder.setProperties(properties);
        ClientConfig config = builder.build();

        // Then
        assertEquals("group.name", "test", config.getGroupConfig().getName());
        assertEquals("group.pass", "1234", config.getGroupConfig().getPassword());
        assertEquals("member", "19.0.0.2:5670", config.getNetworkConfig().getAddresses().iterator().next());
    }

}
