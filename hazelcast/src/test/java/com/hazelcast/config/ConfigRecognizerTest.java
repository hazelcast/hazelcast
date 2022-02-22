/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.config.ClientConfigRecognizer;
import com.hazelcast.client.config.ClientFailoverConfigRecognizer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.util.Scanner;

import static com.hazelcast.internal.util.StringUtil.equalsIgnoreCase;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConfigRecognizerTest {
    static final String HAZELCAST_START_TAG = "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n";
    static final String HAZELCAST_END_TAG = "</hazelcast>\n";
    static final String HAZELCAST_CLIENT_START_TAG =
            "<hazelcast-client xmlns=\"http://www.hazelcast.com/schema/client-config\">\n";
    static final String HAZELCAST_CLIENT_END_TAG = "</hazelcast-client>";
    static final String HAZELCAST_CLIENT_FAILOVER_START_TAG =
            "<hazelcast-client-failover xmlns=\"http://www.hazelcast.com/schema/client-failover-config\">\n";
    static final String HAZELCAST_CLIENT_FAILOVER_END_TAG = "</hazelcast-client-failover>";

    @Test
    public void testRecognizeMemberFullBlownXml() throws Exception {
        ConfigRecognizer memberRecognizer = new MemberConfigRecognizer();
        ConfigRecognizer clientRecognizer = new ClientConfigRecognizer();
        ConfigRecognizer clientFailoverRecognizer = new ClientFailoverConfigRecognizer();

        String xml = ""
                + "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                + "<!--\n"
                + "BANNER\n"
                + "-->\n"
                + "\n"
                + HAZELCAST_START_TAG
                + "  <cluster-name>foobar</cluster-name>\n"
                + HAZELCAST_END_TAG;
        byte[] bytes = xml.getBytes();
        ConfigStream bis = new ConfigStream(new BufferedInputStream(new ByteArrayInputStream(bytes), bytes.length));

        assertTrue(memberRecognizer.isRecognized(bis));
        assertFalse(clientRecognizer.isRecognized(bis));
        assertFalse(clientFailoverRecognizer.isRecognized(bis));
    }

    @Test
    public void testRecognizeMemberFullBlownXmlPartialStreamRead() throws Exception {
        ConfigRecognizer memberRecognizer = new MemberConfigRecognizer();
        ConfigRecognizer clientRecognizer = new ClientConfigRecognizer();
        ConfigRecognizer clientFailoverRecognizer = new ClientFailoverConfigRecognizer();

        String xml = ""
                + "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                + "<!--\n"
                + "BANNER\n"
                + "-->\n"
                + "\n"
                + HAZELCAST_START_TAG  // end of read limit:116 bytes
                + "  <cluster-name>foobar</cluster-name>"; // incomplete config, this line is not consumed by the recognizer
        byte[] bytes = xml.getBytes();
        ConfigStream bis = new ConfigStream(new BufferedInputStream(new ByteArrayInputStream(bytes), bytes.length), 116);

        assertTrue(memberRecognizer.isRecognized(bis));
        assertFalse(clientRecognizer.isRecognized(bis));
        assertFalse(clientFailoverRecognizer.isRecognized(bis));
    }

    @Test
    public void testRecognizeMemberXml() throws Exception {
        ConfigRecognizer memberRecognizer = new MemberConfigRecognizer();
        ConfigRecognizer clientRecognizer = new ClientConfigRecognizer();
        ConfigRecognizer clientFailoverRecognizer = new ClientFailoverConfigRecognizer();

        String xml = HAZELCAST_START_TAG
                + "  <cluster-name>foobar</cluster-name>\n"
                + HAZELCAST_END_TAG;
        ConfigStream bis = new ConfigStream(new ByteArrayInputStream(xml.getBytes()));

        assertTrue(memberRecognizer.isRecognized(bis));
        assertFalse(clientRecognizer.isRecognized(bis));
        assertFalse(clientFailoverRecognizer.isRecognized(bis));
    }

    @Test
    public void testRecognizeClientXml() throws Exception {
        ConfigRecognizer memberRecognizer = new MemberConfigRecognizer();
        ConfigRecognizer clientRecognizer = new ClientConfigRecognizer();
        ConfigRecognizer clientFailoverRecognizer = new ClientFailoverConfigRecognizer();

        String xml = HAZELCAST_CLIENT_START_TAG
                + "  <cluster-name>foobar</cluster-name>\n"
                + HAZELCAST_CLIENT_END_TAG;
        ConfigStream bis = new ConfigStream(new ByteArrayInputStream(xml.getBytes()));

        assertFalse(memberRecognizer.isRecognized(bis));
        assertTrue(clientRecognizer.isRecognized(bis));
        assertFalse(clientFailoverRecognizer.isRecognized(bis));
    }

    @Test
    public void testRecognizeClientXmlPartialStreamRead() throws Exception {
        ConfigRecognizer memberRecognizer = new MemberConfigRecognizer();
        ConfigRecognizer clientRecognizer = new ClientConfigRecognizer();
        ConfigRecognizer clientFailoverRecognizer = new ClientFailoverConfigRecognizer();

        String xml = HAZELCAST_CLIENT_START_TAG // end of read limit:72 bytes
                + "  <cluster-name>foobar</cluster-name>\n"; // incomplete config, this line is not consumed by the recognizers
        byte[] bytes = xml.getBytes();
        ConfigStream bis = new ConfigStream(new BufferedInputStream(new ByteArrayInputStream(bytes), bytes.length), 72);

        assertFalse(memberRecognizer.isRecognized(bis));
        assertTrue(clientRecognizer.isRecognized(bis));
        assertFalse(clientFailoverRecognizer.isRecognized(bis));
    }

    @Test
    public void testRecognizeFailoverClientXml() throws Exception {
        ConfigRecognizer memberRecognizer = new MemberConfigRecognizer();
        ConfigRecognizer clientRecognizer = new ClientConfigRecognizer();
        ConfigRecognizer clientFailoverRecognizer = new ClientFailoverConfigRecognizer();

        String xml = HAZELCAST_CLIENT_FAILOVER_START_TAG
                + "  <cluster-name>foobar</cluster-name>\n"
                + HAZELCAST_CLIENT_FAILOVER_END_TAG;
        ConfigStream bis = new ConfigStream(new ByteArrayInputStream(xml.getBytes()));

        assertFalse(memberRecognizer.isRecognized(bis));
        assertFalse(clientRecognizer.isRecognized(bis));
        assertTrue(clientFailoverRecognizer.isRecognized(bis));
    }

    @Test
    public void testRecognizeFailoverClientXmlPartialStreamRead() throws Exception {
        ConfigRecognizer memberRecognizer = new MemberConfigRecognizer();
        ConfigRecognizer clientRecognizer = new ClientConfigRecognizer();
        ConfigRecognizer clientFailoverRecognizer = new ClientFailoverConfigRecognizer();

        String xml = HAZELCAST_CLIENT_FAILOVER_START_TAG // end of read limit:90 bytes
                + "  <cluster-name>foobar</cluster-name>\n"; // incomplete config, this line is not consumed by the recognizers
        byte[] bytes = xml.getBytes();
        ConfigStream bis = new ConfigStream(new BufferedInputStream(new ByteArrayInputStream(bytes), bytes.length), 90);

        assertFalse(memberRecognizer.isRecognized(bis));
        assertFalse(clientRecognizer.isRecognized(bis));
        assertTrue(clientFailoverRecognizer.isRecognized(bis));
    }

    @Test
    public void testInvalidXmlIsNotRecognized() throws Exception {
        ConfigRecognizer memberRecognizer = new MemberConfigRecognizer();
        ConfigRecognizer clientRecognizer = new ClientConfigRecognizer();
        ConfigRecognizer clientFailoverRecognizer = new ClientFailoverConfigRecognizer();

        String xml = "invalid-xml";
        ConfigStream bis = new ConfigStream(new ByteArrayInputStream(xml.getBytes()));

        assertFalse(memberRecognizer.isRecognized(bis));
        assertFalse(clientRecognizer.isRecognized(bis));
        assertFalse(clientFailoverRecognizer.isRecognized(bis));
    }

    @Test
    public void testRecognizeMemberYaml() throws Exception {
        ConfigRecognizer memberRecognizer = new MemberConfigRecognizer();
        ConfigRecognizer clientRecognizer = new ClientConfigRecognizer();
        ConfigRecognizer clientFailoverRecognizer = new ClientFailoverConfigRecognizer();

        String yaml = ""
                + "hazelcast:\n"
                + "  cluster-name: foobar";
        ConfigStream bis = new ConfigStream(new ByteArrayInputStream(yaml.getBytes()));

        assertTrue(memberRecognizer.isRecognized(bis));
        assertFalse(clientRecognizer.isRecognized(bis));
        assertFalse(clientFailoverRecognizer.isRecognized(bis));
    }

    @Test
    public void testRecognizeMemberYamlPartialStreamRead() throws Exception {
        ConfigRecognizer memberRecognizer = new MemberConfigRecognizer();
        ConfigRecognizer clientRecognizer = new ClientConfigRecognizer();
        ConfigRecognizer clientFailoverRecognizer = new ClientFailoverConfigRecognizer();

        String yaml = ""
                + "hazelcast:\n"
                + "  c"; // ...luster-name: foobar"; "c" is needed to make the YAML document valid
        ConfigStream bis = new ConfigStream(new BufferedInputStream(new ByteArrayInputStream(yaml.getBytes())), 14);

        assertTrue(memberRecognizer.isRecognized(bis));
        assertFalse(clientRecognizer.isRecognized(bis));
        assertFalse(clientFailoverRecognizer.isRecognized(bis));
    }

    @Test
    public void testRecognizeClientYaml() throws Exception {
        ConfigRecognizer memberRecognizer = new MemberConfigRecognizer();
        ConfigRecognizer clientRecognizer = new ClientConfigRecognizer();
        ConfigRecognizer clientFailoverRecognizer = new ClientFailoverConfigRecognizer();

        String yaml = ""
                + "hazelcast-client:\n"
                + "  cluster-name: foobar";
        ConfigStream bis = new ConfigStream(new ByteArrayInputStream(yaml.getBytes()));

        assertFalse(memberRecognizer.isRecognized(bis));
        assertTrue(clientRecognizer.isRecognized(bis));
        assertFalse(clientFailoverRecognizer.isRecognized(bis));
    }

    @Test
    public void testRecognizeClientYamlPartialStreamRead() throws Exception {
        ConfigRecognizer memberRecognizer = new MemberConfigRecognizer();
        ConfigRecognizer clientRecognizer = new ClientConfigRecognizer();
        ConfigRecognizer clientFailoverRecognizer = new ClientFailoverConfigRecognizer();

        String yaml = ""
                + "hazelcast-client:\n"
                + "  c"; // ...luster-name: foobar"; "c" is needed to make the YAML document valid
        ConfigStream bis = new ConfigStream(new BufferedInputStream(new ByteArrayInputStream(yaml.getBytes())), 21);

        assertFalse(memberRecognizer.isRecognized(bis));
        assertTrue(clientRecognizer.isRecognized(bis));
        assertFalse(clientFailoverRecognizer.isRecognized(bis));
    }

    @Test
    public void testRecognizeFailoverClientYaml() throws Exception {
        ConfigRecognizer memberRecognizer = new MemberConfigRecognizer();
        ConfigRecognizer clientRecognizer = new ClientConfigRecognizer();
        ConfigRecognizer clientFailoverRecognizer = new ClientFailoverConfigRecognizer();

        String yaml = ""
                + "hazelcast-client-failover:\n"
                + "  cluster-name: foobar";
        ConfigStream bis = new ConfigStream(new ByteArrayInputStream(yaml.getBytes()));

        assertFalse(memberRecognizer.isRecognized(bis));
        assertFalse(clientRecognizer.isRecognized(bis));
        assertTrue(clientFailoverRecognizer.isRecognized(bis));
    }

    @Test
    public void testRecognizeFailoverClientYamlPartialStreamRead() throws Exception {
        ConfigRecognizer memberRecognizer = new MemberConfigRecognizer();
        ConfigRecognizer clientRecognizer = new ClientConfigRecognizer();
        ConfigRecognizer clientFailoverRecognizer = new ClientFailoverConfigRecognizer();

        String yaml = ""
                + "hazelcast-client-failover:\n"
                + "  c"; // ...luster-name: foobar"; "c" is needed to make the YAML document valid
        ConfigStream bis = new ConfigStream(new BufferedInputStream(new ByteArrayInputStream(yaml.getBytes())), 30);

        assertFalse(memberRecognizer.isRecognized(bis));
        assertFalse(clientRecognizer.isRecognized(bis));
        assertTrue(clientFailoverRecognizer.isRecognized(bis));
    }

    @Test
    public void testInvalidYamlIsNotRecognized() throws Exception {
        ConfigRecognizer memberRecognizer = new MemberConfigRecognizer();
        ConfigRecognizer clientRecognizer = new ClientConfigRecognizer();
        ConfigRecognizer clientFailoverRecognizer = new ClientFailoverConfigRecognizer();

        String yaml = "invalid-yaml";
        ConfigStream bis = new ConfigStream(new ByteArrayInputStream(yaml.getBytes()));

        assertFalse(memberRecognizer.isRecognized(bis));
        assertFalse(clientRecognizer.isRecognized(bis));
        assertFalse(clientFailoverRecognizer.isRecognized(bis));
    }

    @Test
    public void testRecognizeWithCustomConfigRecognizer() throws Exception {
        ConfigRecognizer customRecognizer = new TestConfigRecognizer();
        ConfigRecognizer memberRecognizer = new MemberConfigRecognizer(customRecognizer);
        ConfigRecognizer clientRecognizer = new ClientConfigRecognizer(customRecognizer);
        ConfigRecognizer clientFailoverRecognizer = new ClientFailoverConfigRecognizer(customRecognizer);

        String config = "test-hazelcast-config";
        ConfigStream bis = new ConfigStream(new ByteArrayInputStream(config.getBytes()));

        assertTrue(memberRecognizer.isRecognized(bis));
        assertTrue(clientRecognizer.isRecognized(bis));
        assertTrue(clientFailoverRecognizer.isRecognized(bis));
    }

    private class TestConfigRecognizer implements ConfigRecognizer {

        @Override
        public boolean isRecognized(ConfigStream configStream) throws Exception {
            String firstLine;
            try (Scanner scanner = new Scanner(configStream)) {
                firstLine = scanner.nextLine();
            }

            return equalsIgnoreCase("test-hazelcast-config", firstLine);
        }
    }
}
