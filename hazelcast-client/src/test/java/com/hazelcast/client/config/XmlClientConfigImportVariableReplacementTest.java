/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.hazelcast.client.config;

import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class XmlClientConfigImportVariableReplacementTest {


    @Test(expected = IllegalStateException.class)
    public void testImportElementOnlyAppersInTopLevel() throws Exception {
        String xml = "<hazelcast-client>\n" +
                "   <network>" +
                "        <import resource=\"\"/>\n" +
                "   </network>" +
                "</hazelcast-client>";

        buildConfig(xml);
    }

    @Test(expected = IllegalStateException.class)
    public void testHazelcastElementOnlyAppearsOnce() throws Exception {
        String xml = "<hazelcast-client>\n" +
                "   <hazelcast-client>" +
                "   </hazelcast-client>" +
                "</hazelcast-client>";

        buildConfig(xml);
    }

    @Test
    public void readVariables() {
        String xml =
                "<hazelcast-client>\n" +
                        "<executor-pool-size>${executor.pool.size}</executor-pool-size>" +
                        "</hazelcast-client>";

        ClientConfig config = buildConfig(xml, "executor.pool.size", "40");
        assertEquals(40, config.getExecutorPoolSize());
    }

    @Test
    public void testImportConfigFromResourceVariables() throws IOException {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String networkConfig = "<hazelcast-client>" +
                "<network>" +
                "<cluster-members>" +
                "<address>192.168.100.100</address>" +
                "<address>127.0.0.10</address>" +
                "</cluster-members>" +
                "<smart-routing>false</smart-routing>" +
                "<redo-operation>true</redo-operation>" +

                "<socket-interceptor enabled=\"true\">" +
                "<class-name>com.hazelcast.examples.MySocketInterceptor</class-name>" +
                "<properties>" +
                "<property name=\"foo\">bar</property>" +
                "</properties>" +
                "</socket-interceptor>" +
                "        </network>" +
                "</hazelcast-client>";
        writeStringToStreamAndClose(os, networkConfig);

        String xml = "<hazelcast-client>\n" +
                "    <import resource=\"${config.location}\"/>\n" +
                "</hazelcast-client>";

        ClientConfig config = buildConfig(xml, "config.location", file.getAbsolutePath());
        assertFalse(config.getNetworkConfig().isSmartRouting());
        assertTrue(config.getNetworkConfig().isRedoOperation());
        assertTrue(config.getNetworkConfig().getAddresses().contains("192.168.100.100"));
        assertTrue(config.getNetworkConfig().getAddresses().contains("127.0.0.10"));
    }

    @Test
    public void testImportedConfigVariableReplacement() throws IOException {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String networkConfig = "<hazelcast-client>" +
                "<network>" +
                "<cluster-members>" +
                "<address>${ip.address}</address>" +
                "</cluster-members>" +
                "</network>" +
                "</hazelcast-client>";
        writeStringToStreamAndClose(os, networkConfig);

        String xml = "<hazelcast-client>\n" +
                "    <import resource=\"${config.location}\"/>\n" +
                "</hazelcast-client>";


        Properties properties = new Properties();
        properties.setProperty("config.location", file.getAbsolutePath());
        properties.setProperty("ip.address", "192.168.5.5");
        ClientConfig config = buildConfig(xml, properties);
        assertTrue(config.getNetworkConfig().getAddresses().contains("192.168.5.5"));
    }

    @Test(expected = HazelcastException.class)
    public void testTwoResourceCyclicImportThrowsException() throws Exception {
        File config1 = createConfigFile("hz1", "xml");
        File config2 = createConfigFile("hz2", "xml");
        FileOutputStream os1 = new FileOutputStream(config1);
        FileOutputStream os2 = new FileOutputStream(config2);
        String config1Xml = "<hazelcast-client>" +
                "    <import resource=\"file://" + config2.getAbsolutePath() + "\"/>\n" +
                "</hazelcast-client>";
        String config2Xml = "<hazelcast-client>" +
                "    <import resource=\"file://" + config1.getAbsolutePath() + "\"/>\n" +
                "</hazelcast-client>";
        writeStringToStreamAndClose(os1, config1Xml);
        writeStringToStreamAndClose(os2, config2Xml);

        buildConfig(config1Xml);
    }

    @Test(expected = HazelcastException.class)
    public void testThreeResourceCyclicImportThrowsException() throws Exception {
        File config1 = createConfigFile("hz1", "xml");
        File config2 = createConfigFile("hz2", "xml");
        File config3 = createConfigFile("hz3", "xml");
        FileOutputStream os1 = new FileOutputStream(config1);
        FileOutputStream os2 = new FileOutputStream(config2);
        FileOutputStream os3 = new FileOutputStream(config2);
        String config1Xml = "<hazelcast-client>" +
                "    <import resource=\"file://" + config2.getAbsolutePath() + "\"/>\n" +
                "</hazelcast-client>";
        String config2Xml = "<hazelcast-client>" +
                "    <import resource=\"file://" + config3.getAbsolutePath() + "\"/>\n" +
                "</hazelcast-client>";
        String config3Xml = "<hazelcast-client>" +
                "    <import resource=\"file://" + config1.getAbsolutePath() + "\"/>\n" +
                "</hazelcast-client>";
        writeStringToStreamAndClose(os1, config1Xml);
        writeStringToStreamAndClose(os2, config2Xml);
        writeStringToStreamAndClose(os3, config3Xml);
        buildConfig(config1Xml);
    }

    @Test(expected = HazelcastException.class)
    public void testImportEmptyResourceContent() throws Exception {
        File config1 = createConfigFile("hz1", "xml");
        FileOutputStream os1 = new FileOutputStream(config1);
        String config1Xml = "<hazelcast-client>" +
                "    <import resource=\"file://" + config1.getAbsolutePath() + "\"/>\n" +
                "</hazelcast-client>";
        writeStringToStreamAndClose(os1, "");
        buildConfig(config1Xml);
    }

    @Test(expected = HazelcastException.class)
    public void testImportEmptyResourceThrowsException() throws Exception {
        String xml = "<hazelcast-client>\n" +
                "    <import resource=\"\"/>\n" +
                "</hazelcast-client>";

        buildConfig(xml);
    }

    @Test(expected = HazelcastException.class)
    public void testImportNotExistingResourceThrowsException() throws Exception {
        String xml = "<hazelcast-client>\n" +
                "    <import resource=\"notexisting.xml\"/>\n" +
                "</hazelcast-client>";

        buildConfig(xml);
    }

    @Test
    public void testImportGroupConfigFromClassPath() throws Exception {
        String xml = "<hazelcast-client>\n" +
                "    <import resource=\"classpath:hazelcast-client-c1.xml\"/>\n" +
                "</hazelcast-client>";

        ClientConfig config = buildConfig(xml);
        GroupConfig groupConfig = config.getGroupConfig();
        assertEquals("cluster1", groupConfig.getName());
        assertEquals("cluster1pass", groupConfig.getPassword());
    }

    private File createConfigFile(String filename, String suffix) throws IOException {
        File file = File.createTempFile(filename, suffix);
        file.setWritable(true);
        file.deleteOnExit();
        return file;
    }

    private void writeStringToStreamAndClose(FileOutputStream os, String string) throws IOException {
        os.write(string.getBytes());
        os.flush();
        os.close();
    }

    ClientConfig buildConfig(String xml, Properties properties) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlClientConfigBuilder configBuilder = new XmlClientConfigBuilder(bis);
        configBuilder.setProperties(properties);
        ClientConfig config = configBuilder.build();
        return config;
    }

    ClientConfig buildConfig(String xml, String key, String value) {
        Properties properties = new Properties();
        properties.setProperty(key, value);
        return buildConfig(xml, properties);
    }

    ClientConfig buildConfig(String xml) {
        return buildConfig(xml, null);
    }

}
